import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.*;

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



public class BcryptServiceHandler implements BcryptService.Iface {
    private ConcurrentLinkedQueue<BackendNode> backendNodes;
    private TProtocolFactory protocolFactory;
    private TAsyncClientManager clientManager;
    private Map<BackendNode, BcryptService.AsyncClient> clients;
    private Map<BackendNode, TNonblockingTransport> transports;
    private Logger log;
    private ExecutorService executor;

    private static int NUM_OF_BE_THREADS;

    public BcryptServiceHandler() {
        log = Logger.getLogger(BcryptServiceHandler.class.getName());
        try {
            clientManager = new TAsyncClientManager();
        } catch (Exception e) {
            // TODO: Handle ClientManager initialization failure.
        }

        backendNodes = new ConcurrentLinkedQueue<BackendNode>();
        protocolFactory = new TCompactProtocol.Factory();
        clients = new HashMap<BackendNode, BcryptService.AsyncClient>();
        transports = new HashMap<BackendNode, TNonblockingTransport>();
        NUM_OF_BE_THREADS = BENode.NUM_OF_WORKER_THREADS;
        executor = Executors.newFixedThreadPool(10);
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
        BcryptService.AsyncClient c = clients.get(node);
        TNonblockingTransport t = transports.get(node);
        CompletableFuture<List<String>> fut = new CompletableFuture<List<String>>();
        AsyncMethodCallback<List<String>> callback = new AsyncMethodCallback<List<String>>() {

            @Override
            public void onComplete(List<String> response) {
                fut.complete(response);
            }

            @Override
            public void onError(Exception e) {
                e.printStackTrace();
            }
        };
        c.hashPasswordCompute(password, logRounds, callback);
        try {
            List<String> ret = fut.get();
            backendNodes.add(node);
            return ret;
        } catch (Exception e) {return null;}
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
        BackendNode node = backendNodes.poll();
        if (node == null) {
            return checkPasswordCompute(password, hash);
        }
        log.info("sending to BENode " + node.getHostName() + ":" + node.getPort());
        BcryptService.AsyncClient c = clients.get(node);
        TNonblockingTransport t = transports.get(node);
        CompletableFuture<List<Boolean>> fut = new CompletableFuture<List<Boolean>>();
        AsyncMethodCallback<List<Boolean>> callback = new AsyncMethodCallback<List<Boolean>>() {

            @Override
            public void onComplete(List<Boolean> response) {
                fut.complete(response);
            }

            @Override
            public void onError(Exception e) {
                e.printStackTrace();
            }
        };
        c.checkPasswordCompute(password, hash, callback);
        try {
            List<Boolean> ret = fut.get();
            backendNodes.add(node);
            return ret;
        } catch (Exception e) {return null;}
    }

    public List<String> hashPasswordCompute(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
        log.info("computing hash of " + password.toString());
        try {
            // Split up this work into various threads
            log.info(password.size());
            log.info(Math.ceil(password.size()/4.0));
            double splitSize = 4.0;

            List<Future<List<String>>> workers = new LinkedList<>();
            // create a new thread for each split
            for (int i = 0; i < Math.ceil(password.size() / splitSize); i ++) {
                log.info("I:" + i);
                List<String> passwords = new LinkedList<>();
                for (int j = i*(int)splitSize; j <  i*(int)splitSize + (int)splitSize; j++) {
                    if (password.size() - 1 >= j) {
                        log.info(j + " adding");
                        passwords.add(password.get(j));
                    }
                    else {
                        log.info("breaking");
                        break;
                    }
                }

                Callable<List<String>> workerCallable = new Callable<List<String>>() {
                    @Override
                    public List<String> call() throws Exception {
                        List<String> ret = new ArrayList<>();
                        for (String onePwd : passwords) {
                            String oneHash = BCrypt.hashpw(onePwd, BCrypt.gensalt(logRounds));
                            log.info(onePwd + " adding hash");
                            ret.add(oneHash);
                        }
                        return ret;
                    }
                };

                workers.add(executor.submit(workerCallable));
            }

            List<String> output = new LinkedList<>();
            for (Future<List<String>> task : workers) {
                output.addAll(task.get());
            }

            return output;
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
}
