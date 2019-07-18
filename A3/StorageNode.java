import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;

import org.apache.log4j.*;

public class StorageNode implements CuratorWatcher {
    static Logger log;

    String hostName;
    Integer port;
    String zkConnectString;
    String zkNode;
    CuratorFramework curClient;
    volatile InetSocketAddress primaryAddress;
    KeyValueHandler handler;

    public StorageNode(String hostName, Integer port, String zkConnectString, String zkNode) {
        this.hostName = hostName;
        this.port = port;
        this.zkConnectString = zkConnectString;
        this.zkNode = zkNode;
        primaryAddress = null;
    }

    public void start() throws Exception {
        curClient =
                CuratorFrameworkFactory.builder()
                        .connectString(zkConnectString)
                        .retryPolicy(new RetryNTimes(10, 1000))
                        .connectionTimeoutMs(1000)
                        .sessionTimeoutMs(10000)
                        .build();

        curClient.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                curClient.close();
            }
        });

        handler = new KeyValueHandler(hostName, port, curClient, zkNode);
        KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(handler);
        TServerSocket socket = new TServerSocket(port);
        TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
        sargs.protocolFactory(new TBinaryProtocol.Factory());
        sargs.transportFactory(new TFramedTransport.Factory());
        sargs.processorFactory(new TProcessorFactory(processor));
        sargs.maxWorkerThreads(64);
        TServer server = new TThreadPoolServer(sargs);
        log.info("Launching server");
        new Thread(new Runnable() {
            public void run() {
                server.serve();
            }
        }).start();

        curClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(zkNode + "/", (hostName + ":" + port).getBytes());
        primaryAddress = getPrimary();
        handler.updatePrimary(primaryAddress.getHostName(), primaryAddress.getPort());
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        log = Logger.getLogger(StorageNode.class.getName());

        if (args.length != 4) {
            System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
            System.exit(-1);
        }
        log.info("host: " + args[0]);
        log.info("port: " + args[1]);
        StorageNode node = new StorageNode(args[0], Integer.parseInt(args[1]), args[2], args[3]);
        node.start();
    }

    public InetSocketAddress getPrimary() throws Exception {
        while (true) {
            curClient.sync();
            List<String> children =
                    curClient.getChildren().usingWatcher(this).forPath(zkNode);
            if (children.size() == 0) {
                log.error("No primary found");
                Thread.sleep(100);
                continue;
            }
            Collections.sort(children);
            byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
            String strData = new String(data);
            String[] primary = strData.split(":");
            log.info("Found primary " + strData);
            return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
        }
    }

    synchronized public void process(WatchedEvent event) {
        log.info("ZooKeeper event " + event);
        try {
            primaryAddress = getPrimary();
            handler.updatePrimary(primaryAddress.getHostName(), primaryAddress.getPort());
        } catch (Exception e) {
            log.error("Unable to determine primary");
        }
    }
}
