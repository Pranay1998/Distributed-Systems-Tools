import java.io.*;
import java.net.*;
import java.util.Scanner;
import java.nio.charset.*;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.MutableGraph;

import java.util.Iterator;

import java.util.List;
import java.util.Collections;
import java.util.Arrays;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;

class CCServer {
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



				//String output = "1 2\n3 4\n5 6\n";
				String output = processGraphData(payloadText);

				
				DataOutputStream dout = new DataOutputStream(sock.getOutputStream());
				byte[] result = output.getBytes("UTF-8");
				dout.writeInt(result.length);
				dout.write(result);
				dout.flush();

		    } catch (Exception e) {
				e.printStackTrace();
		    }
		}
    }

    public static String processGraphData(String data) {
		MutableGraph<Integer> graph = GraphBuilder
			.undirected()
			.build();
		Scanner sc = new Scanner(data);
		int first = -1;
    	while(sc.hasNextInt()) {
			if (first == -1) {
				first = sc.nextInt();
			} else {
				graph.putEdge(first, sc.nextInt());
				first = -1;
			}
		}
		sc.close();

		Set<EndpointPair<Integer>> pairs = graph.edges();

		List<Set<EndpointPair<Integer>>> edgesList = split(pairs, 2);

		Set<EndpointPair<Integer>> set1 = edgesList.get(0);
		Set<EndpointPair<Integer>> set2 = edgesList.get(1);

		StringBuffer buffer = new StringBuffer();

		Set<List<Integer>> triangles = Sets.newConcurrentHashSet();

		Runnable run1 = 
			new Runnable(){
				public void run() {
					for (EndpointPair<Integer> pair: set1) {
						Set<Integer> a = graph.adjacentNodes(pair.nodeU());
						Set<Integer> b = graph.adjacentNodes(pair.nodeV());
						Set<Integer> common = Sets.intersection(a,b);
						
						common.forEach(i -> {
							List<Integer> list = Arrays.asList(pair.nodeU(), pair.nodeV(), i);
							Collections.sort(list);
							if (!triangles.contains(list)) {
								triangles.add(list);
								buffer.append(list.get(0) + " " + list.get(1) + " " + list.get(2) + "\n");
							}
						});
					}
				}
			};

		Runnable run2 = 
			new Runnable() {
				public void run() {
					for (EndpointPair<Integer> pair: set2) {
						Set<Integer> a = graph.adjacentNodes(pair.nodeU());
						Set<Integer> b = graph.adjacentNodes(pair.nodeV());
						Set<Integer> common = Sets.intersection(a,b);
						
						common.forEach(i -> {
							List<Integer> list = Arrays.asList(pair.nodeU(), pair.nodeV(), i);
							Collections.sort(list);
							if (!triangles.contains(list)) {
								triangles.add(list);
								buffer.append(list.get(0) + " " + list.get(1) + " " + list.get(2) + "\n");
							}
						});
					}
				}
			};
		
		
		Thread thread1 = new Thread(run1);
		Thread thread2 = new Thread(run2);

		thread1.start();
		thread2.start();

		try {
			thread1.join();
			thread2.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
 
		return buffer.toString();
	}

	public static <T> List<Set<T>> split(Set<T> original, int count) {
		ArrayList<Set<T>> result = new ArrayList<Set<T>>(count);
	
		Iterator<T> it = original.iterator();
	
		int each = original.size() / count;

		for (int i = 0; i < count; i++) {
			HashSet<T> s = new HashSet<T>(original.size() / count + 1);
			result.add(s);
			for (int j = 0; j < each && it.hasNext(); j++) {
				s.add(it.next());
			}
		}
		return result;
	}


}
