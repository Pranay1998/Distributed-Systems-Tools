import java.net.InetSocketAddress;
import java.util.*;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;

import org.apache.log4j.*;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

public class StorageNode {
    static Logger log;
    private CuratorFramework curClient;
    private final String zkNode;
    private final String host;
    private final int port;
    private final String zkConnectString;
    private KeyValueHandler handler;

    public static void main(String [] args) throws Exception {
		BasicConfigurator.configure();
		log = Logger.getLogger(StorageNode.class.getName());

		if (args.length != 4) {
		    System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
		    System.exit(-1);
		}

		StorageNode node = new StorageNode(args[2], args[3], args[0], Integer.parseInt(args[1]));
		node.start();
    }

	public StorageNode(String zkConnectString, String zkNode, String host, int port) {
		this.zkConnectString = zkConnectString;
		this.zkNode = zkNode;
		this.host = host;
		this.port = port;
		this.handler = null;
	}

    void start() throws Exception {
		curClient =
			CuratorFrameworkFactory.builder()
				.connectString(zkConnectString)
				.retryPolicy(new RetryNTimes(10, 1000))
				.connectionTimeoutMs(1000)
				.sessionTimeoutMs(10000)
				.build();

		curClient.start();


		handler = new KeyValueHandler(curClient, zkNode, host, port);
		KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(handler);
		TServerSocket socket = new TServerSocket(port);
		TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory(Integer.MAX_VALUE));
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);
		TServer server = new TThreadPoolServer(sargs);
		log.info("Launching server");

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				curClient.close();
				socket.close();
			}
		});

		new Thread(new Runnable() {
			public void run() {
				server.serve();
			}
		}).start();

		String dataPayload = host + ":" + port;
		curClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(zkNode + "/", dataPayload.getBytes());

		handler.process(null);
	}
}
