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
    private Boolean isPrimary;

    private String backupHost;
    private int backupPort;
    private Boolean isBackup;

    private ReadWriteLock rwLock;
    private Boolean syncedWithPrimary = false;

    public KeyValueHandler(String myHost, int myPort, CuratorFramework curClient, String zkNode) {
        this.myHost = myHost;
        this.myPort = myPort;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<String, String>();
        rwLock = new ReentrantReadWriteLock(true);
    }

    public Map<String, String> getAll() throws org.apache.thrift.TException {
        // TODO: Do we need to synchronize this return?
        // TODO: Is it okay to return entire map? Or is there too much overhead?
        return myMap;
    }

    public String get(String key) throws org.apache.thrift.TException {
        String ret = null;

        rwLock.readLock().lock();
        ret = myMap.get(key);
        rwLock.readLock().unlock();

        if (ret == null) {
            return "";
        } else {
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

    public void updateBackup(String hostName, int portNumber) {
        // System.out.println("Backup: " + hostName + ":" + portNumber);
        backupHost = hostName;
        backupPort = portNumber;
        isBackup = (myHost.equals(backupHost) && myPort == backupPort);
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
        myMap = new ConcurrentHashMap<String, String>(getPrimaryKeyValueClient().getAll());    // sync with primary
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

    KeyValueService.Client getBackupKeyValueClient() {
        while (true) {
            try {
                TSocket sock = new TSocket(backupHost, backupPort);
                TTransport transport = new TFramedTransport(sock);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                return new KeyValueService.Client(protocol);
            } catch (Exception e) {
                System.out.println("Unable to connect to backup");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
    }
}
