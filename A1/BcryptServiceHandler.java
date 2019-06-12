import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.lang.Thread;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {
    private LinkedList<Map<String, Integer>> backendNodes;

    public BcryptServiceHandler() {
        backendNodes = new LinkedList<Map<String,Integer>>();
    }

    // TODO: Load balancing: return the next best available node to do work.
    public Map.Entry getBENode() {
        return backendNodes.getFirst();
    }

    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
        Map.Entry node = getBENode();
        TNonblockingTransport transport = new TNonblockingSocket(node.getKey(), node.getValue());
        TProtocolFactory pf = new TCompactProtocol.Factory();
        BcryptService.AsyncClient cm = new BcryptService.AsyncClient();
        BcryptService.Client client = new BcryptService.Client(pf, cm, transport);
        client.hashPasswordAsyncCallBack()

        Thread thread = new Thread() {
            public void run() {
                try {
                    List<String> ret = new ArrayList<>();
                    for (String onePwd : password) {
                        String oneHash = BCrypt.hashpw(onePwd, BCrypt.gensalt(logRounds));
                        ret.add(oneHash);
                    }

                } catch (Exception e) {
                    throw new IllegalArgument(e.getMessage());
                }
            }
        }.start();
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
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
        Map<String, Integer> m = new HashMap<String, Integer>();
        m.put(hostname, portNumber);
        backendNodes.addLast(m);
    }

    public List<String> hashPasswordAsyncCallBack(List<String> hashedPasswords,
        String hostname, String port) {

    }
}
