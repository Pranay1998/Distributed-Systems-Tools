import java.util.List;
import java.util.Random;
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


	public static int NUM_CLIENTS = 16;
	public static int NUM_PASSWORDS = 5;
	public static int NUM_BATCHES = 5;
	public static short NUM_ROUNDS = (short)0;

    public static void main(String [] args) {
		if (args.length != 3) {
		    System.err.println("Usage: java Client FE_host FE_port");
		    System.exit(-1);
		}

		NUM_ROUNDS = Short.parseShort(args[2]);

		final List<TTransport> transports = new ArrayList<>();
		final List<Thread> threads = new ArrayList<>();
		final List<List<Boolean>> checks = new ArrayList<>();

		for (int i = 0; i < NUM_CLIENTS; i++) {
			final BcryptService.Client client;
			BcryptService.Client tmp = null;


			try {
				final TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
				transports.add(new TFramedTransport(sock));
				final TProtocol protocol = new TBinaryProtocol(transports.get(i));
				tmp = new BcryptService.Client(protocol);
				transports.get(i).open();
			} catch (Exception e) {
				tmp = null;
				e.printStackTrace();
			}
			client = tmp;

			final List<List<String>> list = new ArrayList<>();
			for (int j = 0; j < 1; j++) {
				List<String> pwd = new ArrayList<>();
				list.add(pwd);
			}

			threads.add(new Thread(new Runnable() {
				@Override
				public void run() {
					for (final List<String> l : list) {
						try {
							List<String> hash = client.hashPassword(l, (short)NUM_ROUNDS);
							checks.add(client.checkPassword(l, hash));
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}));
		}

		long start = System.currentTimeMillis();

		for (Thread t: threads) {
			t.start();
		}
	
		try {
			for (Thread t: threads) {
				t.join();
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		} 
		

		long end = System.currentTimeMillis();

		float throughput = (1000f * NUM_BATCHES * NUM_PASSWORDS * NUM_CLIENTS)/(end-start);

		System.out.println("Time taken : " + (end-start) + "ms");
		System.out.println("Throughput with s-"+ NUM_ROUNDS + "  is " + throughput);

		for (TTransport t: transports) {
			t.close();
		}

		System.out.println(checks.size());
		for (List<Boolean> l: checks) {
			for (boolean b: l) {
				if (!b) {
					System.out.println("WRONG");
				}
			}
		}
	}

	public static String randomAlphaNumeric() {
		byte bytes[] = new byte[1024];
		Random rand = new Random();
		rand.nextBytes(bytes);
		return new String(bytes);
	}

}