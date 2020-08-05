import java.util.List;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

import com.google.common.collect.Sets;

class CCServer {

	private static Map<Integer, Set<Integer>> adjacencyMap = new ConcurrentHashMap<>();
	private static StringBuffer buffer = new StringBuffer();
	private static String edges[];
	private static ForkJoinPool customThreadPool = new ForkJoinPool(2);
	
    public static void main(String args[]) throws Exception {
		if (args.length != 1) {
		    System.out.println("usage: java CCServer port");
		    System.exit(-1);
		}
		int port = Integer.parseInt(args[0]);

		ServerSocket ssock = new ServerSocket(port);
		System.out.println("listening on port " + port);
		while(true) {
		    try {
				Socket sock = ssock.accept();
				
				DataInputStream din = new DataInputStream(sock.getInputStream());
				int dataLen = din.readInt();
				System.out.println("received header, data payload has length " + dataLen);
				byte[] payload = new byte[dataLen];
				din.readFully(payload);
				System.out.println("received " + payload.length + " bytes of payload data from server");
				String payloadText = new String(payload, StandardCharsets.UTF_8);

				String output = (dataLen == 0 || payloadText.equals("")) ? "" : processGraphData(payloadText);

				DataOutputStream dout = new DataOutputStream(sock.getOutputStream());
				byte[] result = output.getBytes("UTF-8");
				dout.writeInt(result.length);
				dout.write(result);
				dout.flush();

				System.out.println("sent " + result.length + " bytes of payload data to client");

				adjacencyMap = new ConcurrentHashMap<>();
				buffer = new StringBuffer();
				customThreadPool = new ForkJoinPool(2);
		    } catch (Exception e) {
				e.printStackTrace();
				ssock.close();
		    }
		}
    }

    public static String processGraphData(String data) throws Exception {
		edges = data.split("\\r?\\n");
		customThreadPool.submit(new EdgeParser()).get();
		customThreadPool.submit(new TriangleDetector()).get();
		customThreadPool.shutdown();
		return buffer.toString();
	}

	static class EdgeParser implements Callable<Void> {
		@Override
		public Void call() {
			Arrays.asList(edges)
			.parallelStream()
			.forEach(edge -> {
				String[] values = edge.split(" ");
				Integer x = Integer.valueOf(values[0]);
				Integer y = Integer.valueOf(values[1]);
				adjacencyMap.putIfAbsent(x, new HashSet<>());
				adjacencyMap.get(x).add(y);
			});
			return null;
		}
	}

	static class TriangleDetector implements Callable<Void> {
		@Override
		public Void call() {
			adjacencyMap.keySet()
			.parallelStream()
			.forEach(x -> {
				Set<Integer> xAdj = adjacencyMap.get(x);
				if (xAdj != null) {
					xAdj.forEach(y -> {
						Set<Integer> yAdj = adjacencyMap.get(y);
						if (yAdj != null) {
							Sets.intersection(xAdj, yAdj).forEach(z -> {
								buffer.append(String.format("%d %d %d\n", x, y, z));
							});
						}
					});
				}
			});
			return null;
		}
	}
}
