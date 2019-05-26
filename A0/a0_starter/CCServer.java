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
                while (!csock.isInputShutdown()) {
                    DataInputStream din = new DataInputStream(csock.getInputStream());
                    int reqDataLen = 0;
                    try {
                        reqDataLen = din.readInt();     // read the next 4 bytes as an int
                    } catch (EOFException e) {
                        System.out.println("connection closed by client");
                        break;
                    }
                    System.out.println("received request header, data payload has length " + reqDataLen);
                    byte[] bytes_in = new byte[reqDataLen];
                    din.readFully(bytes_in);
                    int src = 0;
                    int dest = 0;
                    boolean isSrc = true;
                    Graph g = new Graph();
                    for (int i = 0; i < reqDataLen; i++) {
                        int s = bytes_in[i] - '0';
                        if (s == -16) {
                            isSrc = false;
                        } else if (s == -38) {
                            g.addEdge(new Edge(src, dest));
                            src = 0;
                            dest = 0;
                            isSrc = true;
                        } else {
                            if (isSrc) {
                                src = src*10 + s;
                            } else {
                                dest = dest*10 + s;
                            }
                        }
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
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
