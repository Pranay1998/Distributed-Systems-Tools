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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.google.common.collect.Sets;

class CCServer {

	private static Map<Integer, Set<Integer>> adjacencyMap = new ConcurrentHashMap<>();
	private static Set<List<Integer>> triangles = ConcurrentHashMap.newKeySet();
	private static String edges[];
	private static Pair parsedEdges[];

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

				String output = processGraphData(payloadText);

				DataOutputStream dout = new DataOutputStream(sock.getOutputStream());
				byte[] result = output.getBytes("UTF-8");
				dout.writeInt(result.length);
				dout.write(result);
				dout.flush();

				adjacencyMap = new ConcurrentHashMap<>();
				triangles = ConcurrentHashMap.newKeySet();
		    } catch (Exception e) {
				e.printStackTrace();
		    }
		}
    }

    public static String processGraphData(String data) throws Exception {
		if (data.equals("")) return "";
		long s1 = System.currentTimeMillis();
		edges = data.split("\\r?\\n");
		long s2 = System.currentTimeMillis();
		int numEdges = edges.length;
		parsedEdges = new Pair[numEdges];

		ExecutorService executor = Executors.newFixedThreadPool(2);
		List<Callable<Void>> tasks = new ArrayList<>(2);

		tasks.add(0, new EdgeParser(0, numEdges/2));
		tasks.add(1, new EdgeParser(numEdges/2 + 1, numEdges - 1));

		executor.invokeAll(tasks);

		long s3 = System.currentTimeMillis();

		tasks = new ArrayList<>();
		tasks.add(new TriangleDetector(0, numEdges/2));
		tasks.add(new TriangleDetector(numEdges/2 + 1, numEdges - 1));

		executor.invokeAll(tasks);
		executor.shutdown();

		long s4 = System.currentTimeMillis();

		StringBuilder sb = new StringBuilder();

		for (List<Integer> triangle: triangles) {
			sb.append(String.format("%d %d %d\n", triangle.get(0), triangle.get(1), triangle.get(2)));
		}

		long s5 = System.currentTimeMillis();

		System.out.println("Splitting lines " + (s2-s1));
		System.out.println("Parsing Edges " + (s3-s2));
		System.out.println("Finding Triangles " + (s4-s3));
		System.out.println("Building String " + (s5-s4));

		return sb.toString();
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
				if (!adjacencyMap.containsKey(parsedEdges[i].u)) {
					adjacencyMap.put(parsedEdges[i].u, new HashSet<>());
				}
				if (!adjacencyMap.containsKey(parsedEdges[i].v)) {
					adjacencyMap.put(parsedEdges[i].v, new HashSet<>());
				}
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

				Sets.intersection(uNeighbor, vNeighbor).forEach(val -> {
					List<Integer> list = Arrays.asList(pair.u, pair.v, val);
					Collections.sort(list);
					if (!triangles.contains(list)) {
						triangles.add(list);
					}
				});
			}
			return null;
		}
	}
}
