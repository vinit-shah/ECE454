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

class ClientTransportPair {
    private BcryptService.AsyncClient client;
    private TNonblockingTransport transport;
    public ClientTransportPair(BcryptService.AsyncClient client, TNonblockingTransport transport) {
        this.client = client;
        this.transport = transport;
    }

    public BcryptService.AsyncClient getClient() {
        return this.client;
    }

    public TNonblockingTransport getTransport() {
        return this.transport;
    }
}

class BackendNode {
    private String hostname;
    private int port;
    private ConcurrentHashMap<ClientTransportPair, Boolean> clientsAndTransports;
    private int numRequests;
    final private int MAX_REQUESTS = 4;

    BackendNode(String hostname, int port, ConcurrentHashMap<ClientTransportPair, Boolean> clientsAndTransports) {
        this.hostname = hostname;
        this.port = port;
        this.clientsAndTransports = clientsAndTransports;
        this.numRequests = 0;
    }

    public String getHostName() {
        return this.hostname;
    }

    public int getPort() {
        return this.port;
    }

    public synchronized void incrementRequests() {
        this.numRequests++;
    }

    public synchronized  void decrementRequests(ClientTransportPair cp) {
        this.numRequests--;
        this.clientsAndTransports.put(cp,false); // set the client as false so we know no one is currently using it
    }

    public synchronized int getNumRequests() {
        return this.numRequests;
    }

    public synchronized ClientTransportPair getClientTransportPair() {
        for (Map.Entry<ClientTransportPair, Boolean> entry : clientsAndTransports.entrySet()) {
            if (entry.getValue() == false) { // this client is not busy
                clientsAndTransports.put(entry.getKey(), true);
                return entry.getKey();
            }
        }
        return null;
    }
}

public class BcryptServiceHandler implements BcryptService.Iface {
    private ConcurrentLinkedQueue<BackendNode> backendNodes;
    private TProtocolFactory protocolFactory;
    private TAsyncClientManager clientManager;
    private Logger log;
    private ExecutorService executor;
    final private int MAX_REQUESTS = 4;

    public BcryptServiceHandler() {
        log = Logger.getLogger(BcryptServiceHandler.class.getName());
        try {
            clientManager = new TAsyncClientManager();
        } catch (Exception e) {
            e.printStackTrace();
        }

        backendNodes = new ConcurrentLinkedQueue<BackendNode>();
        protocolFactory = new TCompactProtocol.Factory();
        executor = Executors.newFixedThreadPool(32);
    }

    public synchronized BackendNode getBENode() {
        BackendNode node = backendNodes.poll();
        // get 4 tries to find a node that can take on another request, if they're still busy then return null and compute in the front end
        for (int i = 0; i < 4; i ++) {
            log.info("Polling for a BENode the " + i + " time");
            if (node == null) {
                return null;
            }
            log.info("Node: " + node.getHostName() + ":" + node.getPort() + " currently has " + node.getNumRequests() + " requests");
            if (node.getNumRequests() < MAX_REQUESTS) {
                node.incrementRequests();
                log.info("INCREMENT: Node: " + node.getHostName() + ":" + node.getPort() + " now has has " + node.getNumRequests() + " requests");
                backendNodes.add(node);
                return node;
            }
            backendNodes.add(node);
        }
        return null;
    }

    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
        if (logRounds < 4 || logRounds > 31) {
            throw new IllegalArgument("Bad logRounds");
        }
        if (password.isEmpty()) {
            throw new IllegalArgument("Empty list of passwords");
        }
        while (true) {
            CompletableFuture<List<String>> fut = new CompletableFuture<List<String>>();
            AsyncMethodCallback<List<String>> callback = new AsyncMethodCallback<List<String>>() {

                @Override
                public void onComplete(List<String> response) {
                    fut.complete(response);
                }

                @Override
                public void onError(Exception e) {
                    log.info("Failed within hashPassword");
                    fut.completeExceptionally(e);
                }
            };
            BackendNode node = getBENode();
            if (node == null) {
                log.info("Computing on the front end");
                return hashPasswordCompute(password, logRounds);
            }
            ClientTransportPair cp = node.getClientTransportPair(); // get an available client
            if (cp != null) {
                log.info("sending to BENode " + node.getHostName() + ":" + node.getPort());
                BcryptService.AsyncClient c = cp.getClient();
                TNonblockingTransport t = cp.getTransport();
                c.hashPasswordCompute(password, logRounds, callback);
                try {
                    List<String> ret = fut.get();
                    node.decrementRequests(cp);
                    log.info("DECREMENT: Node: " + node.getHostName() + ":" + node.getPort() + " now has has " + node.getNumRequests() + " requests");
                    return ret;
                } catch (Exception e) {
                    log.info("Failed within future.get in hashPassword");
                    if (t.isOpen()) {
                        log.info("Backend Node " + node.getHostName() + ":" + node.getPort() + " is still open");
                        node.decrementRequests(cp);
                        log.info("DECREMENT: Node: " + node.getHostName() + ":" + node.getPort() + " now has has " + node.getNumRequests() + " requests");
                    } else {
                        log.info("Backend Node " + node.getHostName() + ":" + node.getPort() + " is not open");
                        backendNodes.remove(node);
                        log.info("REMOVE: Node " + node.getHostName() + ":" + node.getPort());
                    }

                }
            }
        }
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
        if (password.isEmpty()) {
            throw new IllegalArgument("Empty list of passwords");
        }
        if (hash.isEmpty()) {
            throw new IllegalArgument("Empty list of hashes");
        }
        while(true) {
            CompletableFuture<List<Boolean>> fut = new CompletableFuture<List<Boolean>>();
            AsyncMethodCallback<List<Boolean>> callback = new AsyncMethodCallback<List<Boolean>>() {

                @Override
                public void onComplete(List<Boolean> response) {
                    fut.complete(response);
                }

                @Override
                public void onError(Exception e) {
                    log.info("Failed within checkPassword");
                    fut.completeExceptionally(e);
                }
            };
            BackendNode node = getBENode();
            if (node == null) {
                log.info("Computing on the front end");
                return checkPasswordCompute(password, hash);
            }
            ClientTransportPair cp = node.getClientTransportPair(); // get an available client
            if (cp != null) {
                log.info("sending to BENode " + node.getHostName() + ":" + node.getPort());
                BcryptService.AsyncClient c = cp.getClient();
                TNonblockingTransport t = cp.getTransport();
                c.checkPasswordCompute(password, hash, callback);
                try {
                    List<Boolean> ret = fut.get();
                    node.decrementRequests(cp);
                    log.info("DECREMENT: Node: " + node.getHostName() + ":" + node.getPort() + " now has has " + node.getNumRequests() + " requests");
                    return ret;
                } catch (Exception e) {
                    log.info("Failed within future.get in checkPassword");
                    if (t.isOpen()) {
                        log.info("Backend Node " + node.getHostName() + ":" + node.getPort() + " is still open");
                        node.decrementRequests(cp);
                        log.info("DECREMENT: Node: " + node.getHostName() + ":" + node.getPort() + " now has has " + node.getNumRequests() + " requests");
                    } else {
                        log.info("Backend Node " + node.getHostName() + ":" + node.getPort() + " is not open");
                        backendNodes.remove(node);
                        log.info("REMOVE: Node " + node.getHostName() + ":" + node.getPort());
                    }

                }
            }
        }
    }

    public List<String> hashPasswordCompute(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
        log.info("computing hash");
        try {
            double splitSize = 4.0;

            List<Future<List<String>>> workers = new LinkedList<>();
            // create a new thread for each split
            for (int i = 0; i < Math.ceil(password.size() / splitSize); i ++) {
                List<String> passwords = new LinkedList<>();
                for (int j = i*(int)splitSize; j <  i*(int)splitSize + (int)splitSize; j++) {
                    if (password.size() - 1 >= j) {
                        passwords.add(password.get(j));
                    }
                    else {
                        break;
                    }
                }

                Callable<List<String>> workerCallable = new Callable<List<String>>() {
                    @Override
                    public List<String> call() throws Exception {
                        List<String> ret = new ArrayList<>();
                        for (String onePwd : passwords) {
                            log.info("Hashing: " + onePwd);
                            String oneHash = BCrypt.hashpw(onePwd, BCrypt.gensalt(logRounds));
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
        log.info("checking password hashes");
        try {
            double splitSize = 4.0;

            List<Future<List<Boolean>>> workers = new LinkedList<>();
            // create a new thread for each split
            for (int i = 0; i < Math.ceil(password.size() / splitSize); i ++) {
                log.info("I:" + i);
                List<String> passwords = new LinkedList<>();
                List<String> hashes = new LinkedList<>();
                for (int j = i*(int)splitSize; j <  i*(int)splitSize + (int)splitSize; j++) {
                    if (password.size() - 1 >= j) {
                        passwords.add(password.get(j));
                        hashes.add(hash.get(j));
                    }
                    else {
                        break;
                    }
                }

                Callable<List<Boolean>> workerCallable = new Callable<List<Boolean>>() {
                    @Override
                    public List<Boolean> call() throws Exception {
                        List<Boolean> ret = new ArrayList<>();
                        for (int i = 0; i < passwords.size(); i++) {
                            String onePwd = passwords.get(i);
                            String oneHash = hashes.get(i);
                            try {
                                ret.add(BCrypt.checkpw(onePwd, oneHash));
                            } catch (IllegalArgumentException e) {
                                log.info("MALFORMED HASH");
                                ret.add(false);
                            } catch (Exception e) {
                                e.printStackTrace();
                                ret.add(false);
                            }
                        }
                        return ret;
                    }
                };

                workers.add(executor.submit(workerCallable));
            }

            List<Boolean> output = new LinkedList<>();
            for (Future<List<Boolean>> task : workers) {
                output.addAll(task.get());
            }

            return output;
        } catch (Exception e) {
            throw new IllegalArgument(e.getMessage());
        }
    }

    public void registerBENode(String hostname, int portNumber) throws IllegalArgument, org.apache.thrift.TException {
        try {
            log.info("registering BE Node on " + hostname + ":" + portNumber);
            ConcurrentHashMap<ClientTransportPair, Boolean> clientsAndTransports = new ConcurrentHashMap<ClientTransportPair, Boolean>();
            for (int i = 0; i < MAX_REQUESTS; i ++) {
                TNonblockingTransport transport = new TNonblockingSocket(hostname, portNumber);
                BcryptService.AsyncClient client = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);
                ClientTransportPair cp = new ClientTransportPair(client, transport);
                clientsAndTransports.put(cp, false);
            }
            BackendNode node = new BackendNode(hostname, portNumber, clientsAndTransports);
            backendNodes.add(node);
        } catch (Exception e) {
        }
    }
}
