import java.util.List;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class Client2 {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: java Client FE_host FE_port password");
            System.exit(-1);
        }

        try {
            TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
            TTransport transport = new TFramedTransport(sock);
            TProtocol protocol = new TBinaryProtocol(transport);
            BcryptService.Client client = new BcryptService.Client(protocol);
            transport.open();

            List<String> password = new ArrayList<>();
            password.add(args[2]);
            for (int i = 0; i < 127; i++) {
                password.add("Client2 " + i);
            }

            long x = System.currentTimeMillis();
            List<String> hash = client.hashPassword(password, (short) 10);
            System.out.println(System.currentTimeMillis() - x);
            // List<Boolean> check = client.checkPassword(password, hash);
            // System.out.println(check.toString());
            transport.close();
        } catch (TException x) {
            x.printStackTrace();
        }
    }
}
