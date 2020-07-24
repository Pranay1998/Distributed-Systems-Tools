import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.log4j.Logger;
import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {
    private final String host;
    private final int port;
    private final Map<String, String> myMap = new ConcurrentHashMap<>();
    private final Map<String, Integer> sequenceMap = new ConcurrentHashMap<>();
    private final CuratorFramework curClient;
    private final String zkNode;
    volatile Role role = Role.OTHER;
    private final Logger log = StorageNode.log;
    volatile boolean solo = true;
    private static final AtomicInteger sequence = new AtomicInteger(0);
    static ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    private static final int CAPACITY = 8;

    private LinkedBlockingQueue<ClientWrapper> backupClientPool = new LinkedBlockingQueue<>(CAPACITY);;

    public enum Role {
        PRIMARY,
        BACKUP,
        OTHER
    }

    public KeyValueHandler(CuratorFramework curClient, String zkNode, String host, int port) {
        this.curClient = curClient;
        this.zkNode = zkNode;
        this.host = host;
        this.port = port;
    }

    public String get(String key) throws org.apache.thrift.TException {
        if (role.equals(Role.PRIMARY)) {
            String ret = myMap.get(key);
            if (ret == null)
                return "";
            else
                return ret;
        }
        else {
                throw new TException("Server is not a primary node");
        }
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        if (role.equals(Role.PRIMARY)) {
            rwLock.readLock().lock();
            myMap.put(key, value);
            if (!solo) {
                ClientWrapper client = null;
                try {
                    client = backupClientPool.poll();
                    if (client != null) {
                        client.client.copy(key, value, sequence.addAndGet(1));
                        backupClientPool.put(client);
                    }
                } catch (Exception e) {
                    log.error(e);
                }
            }
            rwLock.readLock().unlock();
        } else {
            throw new TException("Server is not a primary node");
        }
    }

    public void copy(String key, String value, int seq) {
        if (sequenceMap.containsKey(key)) {
            if (seq >= sequenceMap.get(key)) {
                myMap.put(key, value);
                sequenceMap.put(key, seq);
            }
        } else {
            myMap.put(key, value);
            sequenceMap.put(key, seq);
        }
    }

    public void addToMap(Map<String, String> values) {
        myMap.putAll(values);
    }

    ClientWrapper getThriftClient(String host, int port) {
        while (true) {
            try {
                TSocket sock = new TSocket(host, port);
                TTransport transport = new TFramedTransport(sock);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                return new ClientWrapper(new KeyValueService.Client(protocol), transport, host, port);
            } catch (Exception e) {
                log.error("Unable to connect to primary");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error("Error while initializing backup thrift client");
            }
        }
    }

    public void determineRole(List<String> children) throws Exception {
        if (children.size() == 1) {
            role = Role.PRIMARY;
            log.info("Primary Sever");
        } else if (children.size() == 2) {
            String[] primaryData = (new String(curClient.getData().forPath(zkNode + "/" + children.get(0)))).split(":");

            if (host.equals(primaryData[0]) && (port == Integer.parseInt(primaryData[1]))) {
                role = Role.PRIMARY;
                log.info("Primary Server");
            } else {
                role = Role.BACKUP;
                log.info("Backup Server");
            }
        } else {
            log.error("Too many servers - " + children.size());
        }
    }

    @Override
    synchronized public void process(WatchedEvent watchedEvent) throws Exception {
        try {
            curClient.sync();
            List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
            Collections.sort(children);
            if (children.size() > 2) {
                children = children.subList(children.size() - 2, children.size());
            }
            determineRole(children);

            if (children.size() == 1) {
                solo = true;
            } else {
                solo = false;

                if (role.equals(Role.PRIMARY)) {
                    String[] backupData = (new String(curClient.getData().forPath(zkNode + "/" + children.get(1)))).split(":");
                    InetSocketAddress backupNode = new InetSocketAddress(backupData[0], Integer.parseInt(backupData[1]));

                    if (backupClientPool != null) {
                        for (ClientWrapper client: backupClientPool) {
                            client.transport.close();
                        }
                    }

                    backupClientPool = new LinkedBlockingQueue<>(CAPACITY);

                    for (int i = 0; i < CAPACITY; i++) {
                        ClientWrapper client = getThriftClient(backupNode.getHostName(), backupNode.getPort());
                        backupClientPool.put(client);
                    }

                    try {
                        ClientWrapper client = null;
                        client = backupClientPool.take();
                        rwLock.writeLock().lock();
                        client.client.addToMap(myMap);
                        rwLock.writeLock().unlock();
                        backupClientPool.put(client);

                    } catch (InterruptedException e) {
                        log.error(e.getMessage());
                    }
                }
            }

        } catch (Exception e) {
            log.error("Error processing children " + e);
        }
    }

    class ClientWrapper {
        KeyValueService.Client client;
        TTransport transport;
        String host;
        int port;

        public ClientWrapper(KeyValueService.Client client, TTransport transport, String host, int port) {
            this.client = client;
            this.transport = transport;
            this.host = host;
            this.port = port;
        }

        public void destroy() {
            transport.close();
        }
    }
}
