import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
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


public class KeyValueHandler implements KeyValueService.Iface {
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String myHost;
    private int myPort;
    private String primaryHost;
    private int primaryPort;
    private String backupHost;
    private int backupPort;
    private Boolean isPrimary;
    private ReadWriteLock rwLock;

    private Boolean syncedWithPrimary = false;

    public KeyValueHandler(String myHost, int myPort, CuratorFramework curClient, String zkNode) {
        this.myHost = myHost;
        this.myPort = myPort;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<String, String>();
        rwLock = new ReentrantReadWriteLock();
    }

    public Map<String,String> getAll() throws org.apache.thrift.TException {
        // TODO: Do we need to synchronize this return?
        // TODO: Is it okay to return entire map? Or is there too much overhead?
        return myMap;
    }

    public String get(String key) throws org.apache.thrift.TException {
        rwLock.readLock().lock();
        String ret = myMap.get(key);
        if (ret == null) {
            rwLock.readLock().unlock();
            return "";
        } else {
            rwLock.readLock().unlock();
            return ret;
        }
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        if (isPrimary) {
            rwLock.writeLock().lock();

        } else {
            myMap.put(key, value);
        }
    }

    public void updatePrimary(String hostName, int portNumber) {
        // System.out.println("Primary: " + hostName + ":" + portNumber);
        primaryHost = hostName;
        primaryPort = portNumber;
        isPrimary = (myHost.equals(primaryHost) && myPort == primaryPort);
        if (!isPrimary && !syncedWithPrimary) {
            System.out.println("Trying to sync with primary:");
            System.out.println("\t" + myHost + " : " + myPort + " <-- " + primaryHost + " : " + primaryPort);

            try {
                syncWithPrimary();
            } catch (Exception e) {
                System.out.println("Could not sync with primary");
            }
        }
    }

    private void syncWithPrimary() throws Exception {
        System.out.println("syncing with primary " + primaryHost + " : " + primaryPort);
        myMap = getPrimaryKeyValueClient().getAll();    // sync with primary
        syncedWithPrimary = true;
        System.out.println("syncing done");
    }

    KeyValueService.Client getPrimaryKeyValueClient() {
        while (true) {
            try {
                TSocket sock = new TSocket(primaryHost, primaryPort);
                TTransport transport = new TFramedTransport(sock);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                return new KeyValueService.Client(protocol);
            } catch (Exception e) {
                System.out.println("Unable to connect to primary");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
    }
}
