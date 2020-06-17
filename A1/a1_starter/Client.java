import java.util.List;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.lang.Runnable;

public class Client {
    public static void main(String [] args) {
		if (args.length != 3) {
		    System.err.println("Usage: java Client FE_host FE_port password");
		    System.exit(-1);
		}

		Runnable run = new Runnable() {
			@Override
			public void run() {
				try {
					TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
					TTransport transport = new TFramedTransport(sock);
					TProtocol protocol = new TBinaryProtocol(transport);
					BcryptService.Client client = new BcryptService.Client(protocol);
					transport.open();

					
					List<String> password = new ArrayList<>();

					for (int i = 0; i < 50; i++) {
						password.add("Test String");
					}

					List<String> hash = client.hashPassword(password, (short)10);
					List<Boolean> check = client.checkPassword(password, hash);
					
					transport.close();
				}
				catch (TException e) {
					e.printStackTrace();
				}
			}
		};
		
		Thread thread1 = new Thread(run);
		Thread thread2 = new Thread(run);

		long start = System.currentTimeMillis();
		thread1.start();
		thread2.start();
		try {
			thread1.join();
			thread2.join();

		}
		catch(Exception e) {
			e.printStackTrace();
		} 
		long end = System.currentTimeMillis();

		System.out.println("Time taken : " + (end-start));
	}
}
