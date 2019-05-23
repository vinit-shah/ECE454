import java.io.*;
import java.net.*;
import java.nio.charset.*;
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
            System.out.println("Accepted connection from client");
            DataInputStream din = new DataInputStream(csock.getInputStream());
            int reqDataLen = din.readInt();
            System.out.println("received request header, data payload has length " + reqDataLen);
            byte[] bytes = new byte[reqDataLen];
            din.readFully(bytes);
            String input = new String(bytes, StandardCharsets.UTF_8);
            System.out.println("Input was " + input);
	    }
        catch (Exception e) {
		e.printStackTrace();
	    }
	}
    }
}
