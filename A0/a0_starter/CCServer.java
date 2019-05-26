import java.io.*;
import java.net.*;
import java.util.HashMap;

import helper.Graph;
import helper.Edge;

class CCServer {
    public static void main(String args[]) throws Exception {
        if (args.length != 1) {
            System.out.println("usage: java CCServer port");
            System.exit(-1);
        }
        int port = Integer.parseInt(args[0]);

        ServerSocket ssock = new ServerSocket(port);
        System.out.println("listening on port " + port);
        while (true) {
            try {
		/*
		  YOUR CODE GOES HERE
		  - accept connection from server socket
		  - read requests from connection repeatedly
		  - for each request, compute an output and send a response
		  - each message has a 4-byte header followed by a payload
		  - the header is the length of the payload
		    (signed, two's complement, big-endian)
		  - the payload is a string
		    (UTF-8, big-endian)
		*/
                Socket csock = ssock.accept();
                System.out.println("Accepted connection from client at: " + csock.getPort());
                DataInputStream din = new DataInputStream(csock.getInputStream());
                int reqDataLen = din.readInt();     // read the next 4 bytes as an int
                System.out.println("received request header, data payload has length " + reqDataLen);
                BufferedReader in = new BufferedReader(new InputStreamReader(din, "UTF-8"));  // read as UTF-8
                Graph g = new Graph();
                for (String line = in.readLine(); line != null; line = in.readLine()) {
                    int src = Integer.parseInt(line.substring(0, line.indexOf(" ")));
                    int dest = Integer.parseInt(line.substring(line.indexOf(" ") + 1, line.length()));
                    g.addEdge(new Edge(src, dest));

                    if (!in.ready())
                        break;
                }
                HashMap<Integer, Integer> connectedComponents = g.connectedComponents();
                StringBuilder output = new StringBuilder();
                for (int vertex : connectedComponents.keySet()) {
                    output.append(vertex + " " + connectedComponents.get(vertex) + "\n");
                }
                DataOutputStream dout = new DataOutputStream(csock.getOutputStream());
                byte[] bytes = output.toString().getBytes("UTF-8");
                dout.writeInt(bytes.length);
                dout.write(bytes);
                dout.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
