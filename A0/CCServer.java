import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class CCServer {

	private static Map<Integer, HashSet<Integer>> adjacencyMap = Collections.synchronizedMap(new HashMap<Integer, HashSet<Integer>>());;
	private static Set<List<Integer>> triangles = Collections.synchronizedSet(new HashSet<>());;
	private static StringBuffer buffer = new StringBuffer();
	private static String edges[];
	private static Pair parsedEdges[];

    public static void main(String args[]) throws Exception {
		if (args.length != 1) {
		    System.out.println("usage: java CCServer port");
		    System.exit(-1);
		}
		int port = Integer.parseInt(args[0]);

		ServerSocket ssock = new ServerSocket(port);
		Runtime.getRuntime().addShutdownHook(new Thread(){public void run() {
			try {
				ssock.close();
				System.out.println("The server is shut down!");
			} 
			catch (Exception e) {
				e.printStackTrace(); 
			}
		}});
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

				String output = processGraphData(payloadText, Runtime.getRuntime().availableProcessors());

				DataOutputStream dout = new DataOutputStream(sock.getOutputStream());
				byte[] result = output.getBytes("UTF-8");
				dout.writeInt(result.length);
				dout.write(result);
				dout.flush();

				adjacencyMap = Collections.synchronizedMap(new HashMap<Integer, HashSet<Integer>>());;
				triangles = Collections.synchronizedSet(new HashSet<>());;
				buffer = new StringBuffer();
		    } catch (Exception e) {
				e.printStackTrace();
		    }
		}
    }

    public static String processGraphData(String data, int cores) throws Exception {
		edges = data.split("\n");
		int numEdges = edges.length;
		parsedEdges = new Pair[numEdges];

		int batch_size = numEdges/cores;
		int numThreads = batch_size == 0 ? numEdges : cores;
		if (batch_size == 0) batch_size++;

		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		List<Callable<Void>> tasks = new ArrayList<>();
		

		for (int i = 0; i < numThreads; i++) {
			if (i != numThreads - 1) {
				tasks.add(new EdgeParser(i*batch_size, i*batch_size + batch_size));
			} else {
				tasks.add(new EdgeParser(i*batch_size, numEdges - 1));
			}
		}

		executor.invokeAll(tasks);

		for (int i = 0; i < numThreads; i++) {
			if (i != numThreads - 1) {
				tasks.add(new TriangleDetector(i*batch_size, i*batch_size + batch_size));
			} else {
				tasks.add(new TriangleDetector(i*batch_size, numEdges - 1));
			}
		}

		executor.invokeAll(tasks);
		executor.shutdown();

		return buffer.toString();
	}

	static class Pair {
		public Pair(int u, int v) {
			this.u = u;
			this.v = v;
		}

		Integer u;
		Integer v;
	}

	static class EdgeParser implements Callable<Void> {
		int start;
		int end;

		public EdgeParser(int startRange, int endRange) {
			this.start = startRange;
			this.end = endRange;
		}

		@Override
		public Void call() {
			for (int i  = start; i <= end; i++) {
				String[] values = edges[i].split(" ");
				parsedEdges[i] = new Pair(Integer.valueOf(values[0]), Integer.valueOf(values[1]));
				adjacencyMap.putIfAbsent(parsedEdges[i].u, new HashSet<>());
				adjacencyMap.putIfAbsent(parsedEdges[i].v, new HashSet<>());
				adjacencyMap.get(parsedEdges[i].u).add(parsedEdges[i].v);
				adjacencyMap.get(parsedEdges[i].v).add(parsedEdges[i].u);
			}
			return null;
		}
	}

	static class TriangleDetector implements Callable<Void> {
		int start;
		int end;

		public TriangleDetector(int startRange, int endRange) {
			this.start = startRange;
			this.end = endRange;
		}

		@Override
		public Void call() {
			for (int i = start; i <= end; i++) {
				final Pair pair = parsedEdges[i];
				Set<Integer> uNeighbor = adjacencyMap.get(pair.u);
				Set<Integer> vNeighbor = adjacencyMap.get(pair.v);

				Set<Integer> common = new HashSet<>();
				uNeighbor.forEach(nbr -> {
					if (vNeighbor.contains(nbr)) {
						common.add(nbr);
					}
				});

				common.forEach(val -> {
					List<Integer> list = Arrays.asList(pair.u, pair.v, val);
					Collections.sort(list);
					if (!triangles.contains(list)) {
						triangles.add(list);
						buffer.append(list.get(0) + " " + list.get(1) + " " + list.get(2) + "\n");
					}
				});
			}
			return null;
		}
	}
}
