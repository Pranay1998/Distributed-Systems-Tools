import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.lang.Math;
import java.util.Collections;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.mindrot.jbcrypt.BCrypt;


public class BcryptServiceHandler implements BcryptService.Iface {
	public List<Node> be_nodes = Collections.synchronizedList(new ArrayList<Node>());

	public synchronized Node getLowestWorkloadAndUpdate(short logRounds) {
		Node n = null;

		Iterator<Node> it  = be_nodes.iterator();

		while (it.hasNext()) {
			Node node = it.next();
			if (n == null || node.workload < n.workload) {
				n = node;
			}
		}

		n.workload += Math.pow(2, logRounds);
		return n;
	}

	public class Node {
		BcryptService.Client client1;
		BcryptService.Client client2;
		boolean busy1 = false;
		boolean busy2 = false;

		int workload = 0;
		public Semaphore available = new Semaphore(2, true);

		public Node(BcryptService.Client client1, BcryptService.Client client2) {
			this.client1 = client1;
			this.client2 = client2;
		}

		public synchronized int getFreeClient() throws Exception {
			if (!busy1) {
				busy1 = true;
				return 1;
			} else if (!busy2) {
				busy2 = true;
				return 2;
			} else {
				throw new Exception("Check concurrency logic");
			}
		}
	}

    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
		
		List<String> ret = null;
		boolean done = false;
		Node n = null;

		while (!done) {
			if (be_nodes.size() == 0) {
				try {
					List<String> str =  backendHashPassword(password, logRounds);
					return str;
				} catch (Exception e) {
					throw e;
				}
			}

			try {
				n = getLowestWorkloadAndUpdate(logRounds);

				n.available.acquire();
				int c = n.getFreeClient();
				if (c == 1) {
					ret = n.client1.backendHashPassword(password, logRounds);
					n.busy1 = false;
					done = true;
				} else if (c == 2) {
					ret = n.client2.backendHashPassword(password, logRounds);
					n.busy2 = false;
					done = true;
				}
				n.available.release();

				done = true;
			} catch(Exception e) {
				//e.printStackTrace();
				if (e instanceof IllegalArgument) {
					throw new IllegalArgument(e.getMessage());
				}
				else if (n != null) {
					be_nodes.remove(n);
				}
			}
		}

		return ret;
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {

		List<Boolean> ret = null;
		boolean done = false;
		Node n = null;

	
		while (!done) {

			if (be_nodes.size() == 0) {
				try {
					List<Boolean> check =  backendCheckPassword(password, hash);
					return check;
				} catch (Exception e) {
					throw e;
				}
			}

			try {
				n = getLowestWorkloadAndUpdate((short)8);

				n.available.acquire();
				int c = n.getFreeClient();
				if (c == 1) {
					ret = n.client1.backendCheckPassword(password, hash);
					n.busy1 = false;
					done = true;
				} else if (c == 2) {
					ret = n.client2.backendCheckPassword(password, hash);
					n.busy2 = false;
					done = true;
				}
				n.available.release();

				done = true;
			} catch(Exception e) {
				//e.printStackTrace();
				if (e instanceof IllegalArgument) {
					throw new IllegalArgument(e.getMessage());
				}
				else if (n != null) {
					be_nodes.remove(n);
				}
			}
		}

		return ret;
	}
	
	public List<String> backendHashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
		try {
			if (password == null || password.size() == 0) {
				throw new Exception("Password list is either null or has no elements in it.");
			}

			if (logRounds < 4 || logRounds > 30) {
				throw new Exception("logRounds of rounds out of range");
			}

		    List<String> ret = new ArrayList<>();
		    for (int i = 0; i < password.size(); i++) {
		    	ret.add(BCrypt.hashpw(password.get(i), BCrypt.gensalt(logRounds)));
		    }
		    return ret;
		} catch (Exception e) {
			//e.printStackTrace();
		    throw new IllegalArgument(e.getMessage());
		}
    }

    public List<Boolean> backendCheckPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
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
			//e.printStackTrace();
		    throw new IllegalArgument(e.getMessage());
		}
    }

    public boolean addNode(String hostName, int port) throws IllegalArgument, org.apache.thrift.TException {
		try {
			TSocket sock1 = new TSocket(hostName, port);
			TTransport transport1 = new TFramedTransport(sock1);
			TProtocol protocol1 = new TBinaryProtocol(transport1);
			BcryptService.Client client1 = new BcryptService.Client(protocol1);
			transport1.open();

			TSocket sock2 = new TSocket(hostName, port);
			TTransport transport2 = new TFramedTransport(sock2);
			TProtocol protocol2 = new TBinaryProtocol(transport2);
			BcryptService.Client client2 = new BcryptService.Client(protocol2);
			transport2.open();

			Node n = new Node(client1, client2);
			be_nodes.add(n);
			System.out.println("Added backend node, host - " + hostName + " port - " + port);
			return true;
		} catch (Exception e) {
			System.out.println("Could not add backend node, hose - " + hostName + " port - " + port);
			return false;
		}
	}

}