import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.lang.Thread;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

import org.apache.thrift.async.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.mindrot.jbcrypt.BCrypt;

import org.apache.log4j.Logger;

class BackendNode {
    private String hostname;
    private int port;

    BackendNode(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public String getHostName() {
        return this.hostname;
    }

    public int getPort() {
        return this.port;
    }
}

class HashTask implements Callable {
    private List<String> passwords;
    private short logRounds;
    private BcryptService.AsyncClient client;
    private TNonblockingTransport transport;

    public HashTask(List<String> passwords, short logRounds, BcryptService.AsyncClient client, TNonblockingTransport transport) {
        this.passwords = passwords;
        this.logRounds = logRounds;
        this.client = client;
        this.transport = transport;
    }

    @Override
    public List<String> call() {
        CountDownLatch latch = new CountDownLatch(1);
        HashPasswordCallBack ca = new HashPasswordCallBack(latch);
        try {
            client.hashPasswordCompute(passwords, logRounds, ca);
            latch.await();
        } catch (Exception e) {}
        return ca.getResponse();
    }
}

class HashPasswordCallBack implements AsyncMethodCallback<List<String>> {
    private List<String> response;
    private CountDownLatch latch;

    public HashPasswordCallBack(CountDownLatch latch) {
        this.latch = latch;
    }

    public void onComplete(List<String> response) {
        this.response = response;
        latch.countDown();
    }

    public void onError(Exception e) {
        e.printStackTrace();
        latch.countDown();
    }

    public List<String> getResponse() {
        return this.response;
    }
}


public class BcryptServiceHandler implements BcryptService.Iface {
    private ConcurrentLinkedQueue<BackendNode> backendNodes;
    private TProtocolFactory protocolFactory;
    private TAsyncClientManager clientManager;
    private Map<BackendNode, BcryptService.AsyncClient> clients;
    // private Map<BackendNode, BcryptService.Client> clients;
    private Map<BackendNode, TNonblockingTransport> transports;
    private Logger log;
    private ExecutorService executor;

    public BcryptServiceHandler() {
        log = Logger.getLogger(BcryptServiceHandler.class.getName());
        try {
            clientManager = new TAsyncClientManager();
        } catch (Exception e) {}
        backendNodes = new ConcurrentLinkedQueue<BackendNode>();
        protocolFactory = new TCompactProtocol.Factory();
        clients = new HashMap<BackendNode, BcryptService.AsyncClient>();
        transports = new HashMap<BackendNode, TNonblockingTransport>();
        executor = Executors.newFixedThreadPool(4);
    }

    public BackendNode getBENode() {
        return backendNodes.poll();
    }

    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
        BackendNode node = backendNodes.poll();
        if (node == null ) {
            return hashPasswordCompute(password, logRounds);
        }
        log.info("sending to BENode " + node.getHostName() + ":" + node.getPort());
        log.info("list is: " + password.toString());
        BcryptService.AsyncClient c = clients.get(node);
        TNonblockingTransport t = transports.get(node);
        CompletableFuture<List<String>> ret;
        AsyncMethodCallback<List<String>> a = new AsyncMethodCallback<List<String>> () {
            public void onComplete() {

            }
        }
        // Callable<List<String>> callable = new HashTask(password, logRounds, c, t);
        // Future<List<String>> fut = executor.submit(callable);
        // List<String> ret = null;
        // try {
        //     ret = fut.get();
        // } catch (Exception e){}
        // backendNodes.add(node);
        // return ret;
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
        // BackendNode node = getBENode();
        // if (node == null) {
        //     return checkPasswordCompute(password, hash);
        // }
        // BcryptService.AsyncClient ac = clients.get(node);
        // CountDownLatch latch = new CountDownLatch(1);
        // CheckPasswordCallBack ret = new CheckPasswordCallBack(latch);
        // ac.checkPasswordCompute(password, hash, ret);
        // try {
        //     latch.await();
        // }   catch (InterruptedException e) {
        //     System.out.println("Latch exception");
        // }
        // TNonblockingTransport t = transports.get(node);
        //
        // // After node has finished computing, return to the queue to be used for other computations
        // backendNodes.add(node);
        // return ret.getResponse();
        return null;
    }

    public List<String> hashPasswordCompute(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
        log.info("computing hash of " + password.toString());
        try {
            List<String> ret = new ArrayList<>();
            for (String onePwd : password) {
                String oneHash = BCrypt.hashpw(onePwd, BCrypt.gensalt(logRounds));
                log.info(onePwd + " adding hassh");
                ret.add(oneHash);
            }
            return ret;
        } catch (Exception e) {
            throw new IllegalArgument(e.getMessage());
        }
    }

    public List<Boolean> checkPasswordCompute(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
        try {
            List<Boolean> ret = new ArrayList<>();
            if (password.size() != hash.size()) {
                throw new IllegalArgument("password and hash lists were not same length");
            }
            for (int i = 0; i < password.size(); i++) {
                String onePwd = password.get(i);
                String oneHash = hash.get(i);
                ret.add(BCrypt.checkpw(onePwd, oneHash));
            }
            return ret;
        } catch (Exception e) {
            throw new IllegalArgument(e.getMessage());
        }
    }

    public void registerBENode(String hostname, int portNumber) throws IllegalArgument, org.apache.thrift.TException {
        try {
            log.info("registering BE Node on " + hostname + ":" + portNumber);
            BackendNode node = new BackendNode(hostname, portNumber);
            backendNodes.add(node);
            TNonblockingTransport transport = new TNonblockingSocket(hostname, portNumber);
            BcryptService.AsyncClient client = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);
            clients.put(node, client);
            transports.put(node, transport);
        } catch (Exception e) {
        }
    }


    //
    // class CheckPasswordCallBack implements AsyncMethodCallback<List<Boolean>> {
    //     private CountDownLatch latch;
    //     private List<Boolean> response;
    //
    //     public CheckPasswordCallBack(CountDownLatch latch) {
    //         this.latch = latch;
    //     }
    //
    //     public void onComplete(List<Boolean> response) {
    //         this.response = response;
    //         latch.countDown();
    //     }
    //
    //     public void onError(Exception e) {
    //         e.printStackTrace();
    //         latch.countDown();
    //     }
    //
    //     public List<Boolean> getResponse() {
    //         return this.response;
    //     }
    // }
}
