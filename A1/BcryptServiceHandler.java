import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.lang.Thread;
import java.util.concurrent.CountDownLatch;
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
    private LinkedList<BackendNode> backendNodes;
    private TProtocolFactory protocolFactory;
    private TAsyncClientManager clientManager;
    // TODO possible optimization here with clients
    private Map<BackendNode, BcryptService.AsyncClient> clients;
    private Map<BackendNode, TNonblockingTransport> transports;
    private Logger log;

    public BcryptServiceHandler() {
        log = Logger.getLogger(BcryptServiceHandler.class.getName());
        try {
            clientManager = new TAsyncClientManager();
        } catch (Exception e) {}
        backendNodes = new LinkedList<BackendNode>();
        protocolFactory = new TCompactProtocol.Factory();
        clients = new HashMap<BackendNode, BcryptService.AsyncClient>();
        transports = new HashMap<BackendNode, TNonblockingTransport>();
    }

    // TODO: Load balancing: return the next best available node to do work.
    public BackendNode getBENode() {
        return backendNodes.pollFirst();
    }

    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
        BackendNode node = getBENode();
        if (node == null ) {
            log.info("Hashing password in FENode");
            return hashPasswordCompute(password, logRounds); // this is the function that does the computation
        }
        BcryptService.AsyncClient ac = clients.get(node);
        CountDownLatch latch = new CountDownLatch(1);
        HashPasswordCallBack ret = new HashPasswordCallBack(latch);
        ac.hashPasswordCompute(password, logRounds, ret);
        try {
            latch.await();
        } catch (InterruptedException e) {
            System.out.println("Latch exception");
        }
        TNonblockingTransport t = transports.get(node);
        // t.close();
        backendNodes.addLast(node);
        return ret.getResponse();
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
        BackendNode node = getBENode();
        if (node == null) {
            return checkPasswordCompute(password, hash);
        }
        BcryptService.AsyncClient ac = clients.get(node);
        CountDownLatch latch = new CountDownLatch(1);
        CheckPasswordCallBack ret = new CheckPasswordCallBack(latch);
        ac.checkPasswordCompute(password, hash, ret);
        try {
            latch.await();
        }   catch (InterruptedException e) {
            System.out.println("Latch exception");
        }
        TNonblockingTransport t = transports.get(node);
        // t.close();
        backendNodes.addLast(node);
        return ret.getResponse();
    }

    public List<String> hashPasswordCompute(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
        try {
            List<String> ret = new ArrayList<>();
            for (String onePwd : password) {
                String oneHash = BCrypt.hashpw(onePwd, BCrypt.gensalt(logRounds));
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
                if (onePwd.length() != oneHash.length()) {
                    ret.add(false);
                } else {
                    ret.add(BCrypt.checkpw(onePwd, oneHash));
                }

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
            backendNodes.addLast(node);
            TNonblockingTransport transport = new TNonblockingSocket(hostname, portNumber);
            BcryptService.AsyncClient client = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);
            clients.put(node, client);
            transports.put(node, transport);
        } catch (Exception e) {
        }
    }

    class HashPasswordCallBack implements AsyncMethodCallback<List<String>> {
        private CountDownLatch latch;
        private List<String> response;

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

    class CheckPasswordCallBack implements AsyncMethodCallback<List<Boolean>> {
        private CountDownLatch latch;
        private List<Boolean> response;

        public CheckPasswordCallBack(CountDownLatch latch) {
            this.latch = latch;
        }

        public void onComplete(List<Boolean> response) {
            this.response = response;
            latch.countDown();
        }

        public void onError(Exception e) {
            e.printStackTrace();
            latch.countDown();
        }

        public List<Boolean> getResponse() {
            return this.response;
        }
    }
}
