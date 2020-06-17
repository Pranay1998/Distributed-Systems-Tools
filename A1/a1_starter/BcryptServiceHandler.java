import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.mindrot.jbcrypt.BCrypt;


public class BcryptServiceHandler implements BcryptService.Iface {
	public List<Node> be_nodes = new ArrayList<Node>();

	public synchronized Node getLowestWorkloadAndUpdate(short logRounds) {
		Node n = null;

		Iterator<Node> it  = be_nodes.iterator();

		while (it.hasNext()) {
			Node node = it.next();
			if (n == null || node.workload < n.workload) {
				n = node;
			}
		}

		n.workload += 1;
		return n;
	}

	public class Node {
		BcryptService.Client client;
		int workload = 0;
		public Semaphore available = new Semaphore(1, true);

		public Node(BcryptService.Client client) {
			this.client = client;
		}
	}

    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
		List<String> ret = null;
		boolean done = false;
		Node n = null;

		System.out.println("Received request to hash password");

		while (!done) {
			try {
				n = getLowestWorkloadAndUpdate(logRounds);

				n.available.acquire();
				ret = n.client.backendHashPassword(password, logRounds);
				n.available.release();

				done = true;
			} catch(Exception e) {
				updateNodes(n, false);
			}
		}

		return ret;
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
		List<Boolean> ret = null;
		boolean done = false;
		Node n  = null;

		System.out.println("Received request to check password");

		while (!done) {
			try {
				n = getLowestWorkloadAndUpdate((short)0);

				n.available.acquire();
				ret = n.client.backendCheckPassword(password, hash); 
				n.available.release();

				done = true;
			} catch(Exception e) {
				updateNodes(n, false);
			}
		}

		return ret;
	}
	
	public List<String> backendHashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {

		System.out.println("Received hash password request from FE node");

		try {
			if (password == null || password.size() == 0) {
				throw new Exception("Password list is either null or has no elements in it.");
			}

			if (logRounds > 100) {
				throw new Exception("Number of rounds too large");
			}

		    List<String> ret = new ArrayList<>();
		    for (int i = 0; i < password.size(); i++) {
		    	ret.add(BCrypt.hashpw(password.get(i), BCrypt.gensalt(logRounds)));
		    }
		    return ret;
		} catch (Exception e) {
		    throw new IllegalArgument(e.getMessage());
		}
    }

    public List<Boolean> backendCheckPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
		
		System.out.println("Received check password request from FE node");

		try {
			if (password == null || password.size() == 0) {
				throw new Exception("Password list is either null or has no elements in it.");
			}

			if (hash == null || hash.size() == 0) {
				throw new Exception("Hash list is either null or has no elements in it.");
			}

			if (hash.size() != password.size()) {
				throw new Exception("Password and Hash lists must be of the same size.");
			}

		    List<Boolean> ret = new ArrayList<>();
		    for (int i = 0; i < password.size(); i++) {
		    	try {
		    		ret.add(BCrypt.checkpw(password.get(i), hash.get(i)));
		    	} catch (Exception e) {
		    		ret.add(false);
		    	}
		    }
		    return ret;
		} catch (Exception e) {
		    throw new IllegalArgument(e.getMessage());
		}
    }

    public boolean addNode(String hostName, int port) throws IllegalArgument, org.apache.thrift.TException {
		try {
			TSocket sock = new TSocket(hostName, port);
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			BcryptService.Client client = new BcryptService.Client(protocol);
			transport.open();
			Node n = new Node(client);
			updateNodes(n, true);
			System.out.println("Added backend node, host - " + hostName + " port - " + port);
			return true;
		} catch (Exception e) {
			System.out.println("Could not add backend node, hose - " + hostName + " port - " + port);
			return false;
		}
	}

	public synchronized void updateNodes(Node n, boolean add) {
		if (add) be_nodes.add(n);
		else {
			be_nodes.remove(n);
		}
	}
}
