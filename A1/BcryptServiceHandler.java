import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {
    private LinkedList<Map<String, Integer>> backendNodes;

    public BcryptServiceHandler() {
        backendNodes = new LinkedList<Map<String,Integer>>();
    }

    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
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
}
