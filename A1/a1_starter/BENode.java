import java.net.InetAddress;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport; 


public class BENode {
    static Logger log;

    public static void main(String [] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: java BENode FE_host FE_port BE_port");
			System.exit(-1);
		}

		// initialize log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(BENode.class.getName());

		String hostFE = args[0];
		int portFE = Integer.parseInt(args[1]);
		int portBE = Integer.parseInt(args[2]);
		log.info("Launching BE node on port " + portBE + " at host " + getHostName());

		// Configure Thrift server
		BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler());
		TServerSocket socket = new TServerSocket(portBE);
		TSimpleServer.Args sargs = new TSimpleServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		//sargs.maxWorkerThreads(64);
		TSimpleServer server = new TSimpleServer(sargs);

		boolean retry = true;
		TSocket sock;
		TTransport transport;
		TProtocol protocol;
		BcryptService.Client client;
		
		while (retry) {
			System.out.println("Attempting to connect to FE node");
			try {
				sock = new TSocket(hostFE, portFE);
				transport = new TFramedTransport(sock);
				protocol = new TBinaryProtocol(transport);
				client = new BcryptService.Client(protocol);
				transport.open();
				retry = false;
				client.addNode(getHostName(), portBE);
				System.out.println("Connected to FE node");
				transport.close();
			} catch (TException e) {
				Thread.sleep(100);
			}
		}
		
		server.serve();
    }

    static String getHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			return "localhost";
		}
    }
}
