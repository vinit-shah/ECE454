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
    private Map<String, ReadWriteLock> lockMap;
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
        lockMap = new ConcurrentHashMap<String, ReadWriteLock>();
        rwLock = new ReentrantReadWriteLock(true); //
    }

    public Map<String, String> getSnapshot() throws org.apache.thrift.TException {
        System.out.println("KeyValueHandler:getSnapshot");
        System.out.println("KeyValueHandler:getSnapshot lockin on table");
        rwLock.writeLock().lock();
        Map<String, String> ret = myMap;
        rwLock.writeLock().unlock();
        System.out.println("KeyValueHandler:getSnapshot unlocking on table");
        return ret;
    }

    public void sync() {
        System.out.println("KeyValueHandler:sync");
        System.out.println("KeyValueHandler:sync locking on table");
        rwLock.writeLock().lock();
        try {
            KeyValueService.Client primaryClient = getPrimaryKeyValueClient();
            myMap = new ConcurrentHashMap<String,String>(primaryClient.getSnapshot());
        } catch (Exception e) {
            System.out.println("could not sync with primary");
        }
        rwLock.writeLock().unlock();
        System.out.println("KeyValueHandler:sync unlocked table");
    }

    public String get(String key) throws org.apache.thrift.TException {
        System.out.println("KeyValueHandler:get with key: " + key);
        String ret;
        if (!lockMap.containsKey(key)) {
            System.out.println("KeyValueHandler:get creating lock for key: " + key);
            lockMap.put(key, new ReentrantReadWriteLock(true));
        }
        System.out.println("KeyValueHandler:get locking on key: " + key);
        ReadWriteLock keyLock = lockMap.get(key);
        keyLock.readLock().lock();
        ret = myMap.get(key);
        keyLock.readLock().unlock();
        System.out.println("KeyValueHandler:get unlocked on key: " + key);
        if (ret == null) {
            return "";
        } else {
            return ret;
        }
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        System.out.println("KeyValueHandler:put with key: " + key + " value: " + value);
        if (isPrimary) {
            System.out.println("KeyValueHandler:put locking on table");
            rwLock.readLock().lock();
            if (!lockMap.containsKey(key)) {
                System.out.println("KeyValueHandler:put creating lock for key: " + key);
                lockMap.put(key, new ReentrantReadWriteLock(true));
            }
            ReadWriteLock keyLock = lockMap.get(key);
            System.out.println("KeyValueHandler:put locking on key: " + key);
            keyLock.writeLock().lock();
            KeyValueService.Client client = getBackupKeyValueClient();
            if (client != null) {
                client.put(key, value);
            }
            keyLock.writeLock().unlock();
            System.out.println("KeyValueHandler:put unlocked on key: " + key);
            rwLock.readLock().unlock();
            System.out.println("KeyValueHandler:put unlocked on table");
        }
        myMap.put(key,value);
        System.out.println("Finished put with key: " + key + " value: " + value);
    }

    public void updateBackup(String hostName, int portNumber) {
        System.out.println("KeyValueHandler:updateBackup - Backup: " + hostName + ":" + portNumber);
        backupHost = hostName;
        backupPort = portNumber;
        isBackup = (myHost.equals(backupHost) && myPort == backupPort);
    }

    public void updatePrimary(String hostName, int portNumber) {
        System.out.println("KeyValueHandler:updatePrimary - Primary: " + hostName + ":" + portNumber);
        primaryHost = hostName;
        primaryPort = portNumber;
        isPrimary = (myHost.equals(primaryHost) && myPort == primaryPort);
    }

    private KeyValueService.Client getPrimaryKeyValueClient() {
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

    private KeyValueService.Client getBackupKeyValueClient() {
        try {
            TSocket sock = new TSocket(backupHost, backupPort);
            TTransport transport = new TFramedTransport(sock);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            return new KeyValueService.Client(protocol);
        } catch (Exception e) {
            System.out.println("Unable to connect to backup");
        }
        return null;
    }
}
