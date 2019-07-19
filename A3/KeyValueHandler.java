import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.lang.*;
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


public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {
    private Map<String, String> myMap;
    private Map<InetSocketAddress, KeyValueService.Client> backupClients;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;

    private volatile InetSocketAddress primaryAddress;
    private volatile InetSocketAddress backupAddress;

    private ReadWriteLock tableLock;
    private ReadWriteLock[] bucketLocks;


    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
    	this.host = host;
    	this.port = port;
    	this.curClient = curClient;
    	this.zkNode = zkNode;
    	myMap = new ConcurrentHashMap<String, String>();
        backupClients = new ConcurrentHashMap<InetSocketAddress, KeyValueService.Client>();
        tableLock = new ReentrantReadWriteLock(true);
        bucketLocks = new ReentrantReadWriteLock[100];
        for (int i = 0; i < bucketLocks.length; i++) {
            bucketLocks[i] = new ReentrantReadWriteLock(true);
        }
        primaryAddress = null;
        backupAddress = null;
    }

    public String get(String key) throws org.apache.thrift.TException
    {
        System.out.println("KeyValueHandler:get with key: " + key);
        if (!isPrimary()) {
            System.out.println("Client trying to get from backup, throwing an exception at it");
            TSocket sock = new TSocket(primaryAddress.getHostName(), primaryAddress.getPort());
            TTransport transport = new TFramedTransport(sock);
            // this will throw an exception at the client
            transport.open();;
        }
        int bucket = hash(key);
        bucketLocks[bucket].readLock().lock();
    	String ret = myMap.get(key);
        bucketLocks[bucket].readLock().unlock();
    	if (ret == null)
    	    return "";
    	else
    	    return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException
    {
        System.out.println("KeyValueHandler:put with key: " + key + " value: " + value);
        if (!isPrimary()) {
            System.out.println("Client trying to put to backup incorrectly, throwing an exception at it");
            TSocket sock = new TSocket(primaryAddress.getHostName(), primaryAddress.getPort());
            TTransport transport = new TFramedTransport(sock);
            // this will throw an exception at the client
            transport.open();
        }
        int bucket = hash(key);
        tableLock.readLock().lock();
        bucketLocks[bucket].writeLock().lock();
        myMap.put(key,value);
        if (backupAddress != null) {
            try{
                TSocket sock = new TSocket(backupAddress.getHostName(), backupAddress.getPort());
                TTransport transport = new TFramedTransport(sock);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                KeyValueService.Client client = new KeyValueService.Client(protocol);
                client.backupPut(key, value);
                transport.close();
            } catch (Exception e) {
                System.out.println("Failed to write to backup");
                System.out.println(e.getLocalizedMessage());
            }
        } else {
            System.out.println("backup is null");
        }
        bucketLocks[bucket].writeLock().unlock();
        tableLock.readLock().unlock();
    }

    public Map<String,String> copy() throws org.apache.thrift.TException {
        tableLock.writeLock().lock();
        Map<String,String> ret = myMap;
        tableLock.writeLock().unlock();
        return ret;
    }

    public void backupPut(String key, String value) throws org.apache.thrift.TException {
        System.out.println("backupPut with key: " + key + " value: " + value);
        myMap.put(key,value);
    }

    public void sync() {
        tableLock.writeLock().lock();
        while(true) {
            System.out.println("in here");
            try {
                TSocket sock = new TSocket(primaryAddress.getHostName(), primaryAddress.getPort());
                TTransport transport = new TFramedTransport(sock);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                KeyValueService.Client client = new KeyValueService.Client(protocol);
                Map<String,String> temp = client.copy();
                // transport.close();
                myMap = new ConcurrentHashMap<String,String>(temp);
                break;
            } catch(Exception e) {
                System.out.println("Failed to sync with primary");
                System.out.println(e.getLocalizedMessage());
            }
        }
        tableLock.writeLock().unlock();
    }

    synchronized public void determineNodes() throws Exception {
        List<String> children;
        while(true) {
            curClient.sync();
            children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
            if (children.size() == 0) {
                System.out.println("No primary found");
                Thread.sleep(100);
                continue;
            }
            if (children.size() >= 3) {
                System.out.println("Zookeeper hasn't finished deleting the old crashed node");
                Thread.sleep(100);
                continue;
            }
            Collections.sort(children);
            byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
            String strData = new String(data);
            String[] primary = strData.split(":");
            primaryAddress = new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
            System.out.println("Found primary " + strData);
            System.out.println("Children size: " + children.size());
            backupAddress = null;
            if (children.size() > 1) {
                // copy immediately, process stuff later the primary may die quickly and string operations are expensive
                if (!isPrimary()) {
                    sync();
                }
                byte[] backupData = curClient.getData().forPath(zkNode + "/" + children.get(1));
                String backupStrData = new String(backupData);
                String[] backup = backupStrData.split(":");
                backupAddress = new InetSocketAddress(backup[0], Integer.parseInt(backup[1]));
                // registerBackupClient(backupAddress);
                System.out.println("Found backup " + backupStrData);
            }
            break;
        }
    }

    synchronized public void process(WatchedEvent event) {
        System.out.println("ZooKeeper event " + event);
        try {
            determineNodes();
        } catch (Exception e) {
            System.out.println("Unable to determine nodes");
        }
    }

    private int hash(String key) {
        return Math.abs(key.hashCode()%bucketLocks.length);
    }

    synchronized private void registerBackupClient(InetSocketAddress backupAddress) {
        try {
            TSocket sock = new TSocket(backupAddress.getHostName(), backupAddress.getPort());
            TTransport transport = new TFramedTransport(sock);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            KeyValueService.Client client = new KeyValueService.Client(protocol);
            backupClients.put(backupAddress, client);
        } catch (Exception e) {
            System.out.println("Failed to registerBackupClient");
        }
    }

    private synchronized Boolean isPrimary() {
        return (host.equals(primaryAddress.getHostName()) && port == primaryAddress.getPort());
    }
}
