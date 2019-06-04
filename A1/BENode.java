import java.net.InetAddress;
import java.net.Socket;

import BcryptService;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;


public class BENode {
    static Logger log;
    static int NUM_OF_WORKER_THREADS = 8;

    public static void main(String[] args) throws Exception {
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

        // launch Thrift server
        BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler());

        // Half-Sync-Half-Async server: one thread for network I/O and workers <= NUM_OF_WORKER_THREADS
        TNonblockingServerSocket socket = new TNonblockingServerSocket(portBE);
        THsHaServer.Args sargs = new THsHaServer.Args(socket);

        // Set args for multi-threaded server: compact protocol, framed
        sargs.protocolFactory(new TCompactProtocol.Factory());
        sargs.transportFactory(new TFramedTransport.Factory());
        sargs.processorFactory(new TProcessorFactory(processor));

        // TODO: Determine max worker threads we want for effective load balance. Rmr this is an upperbound NOT fixed.
        sargs.maxWorkerThreads(NUM_OF_WORKER_THREADS);

        TServer server = new THsHaServer(sargs);
        server.serve();

        // TODO: Contact the FE layer to alert it that this new node is alive ready to serve
//        while (true) {
//            TTransport sc = socket.accept();
//            while (sc.peek()) {
//                sc.write("Connected");
//            }
//        }
    }

    static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }
}
