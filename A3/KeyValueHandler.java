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

    private ReadWriteLock[] bucketLocks;
    private KeyValueService.Client[] clientPool;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
    	this.host = host;
    	this.port = port;
    	this.curClient = curClient;
    	this.zkNode = zkNode;
    	myMap = new ConcurrentHashMap<String, String>();
        backupClients = new ConcurrentHashMap<InetSocketAddress, KeyValueService.Client>();
        bucketLocks = new ReentrantReadWriteLock[62];
        for (int i = 0; i < bucketLocks.length; i++) {
            bucketLocks[i] = new ReentrantReadWriteLock(true);
        }
        clientPool = new KeyValueService.Client[62];
        primaryAddress = null;
        backupAddress = null;
    }

    public String get(String key) throws org.apache.thrift.TException
    {
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
        if (!isPrimary()) {
            System.out.println("Client trying to put to backup incorrectly, throwing an exception at it");
            TSocket sock = new TSocket(primaryAddress.getHostName(), primaryAddress.getPort());
            TTransport transport = new TFramedTransport(sock);
            // this will throw an exception at the client
            transport.open();
        }
        int bucket = hash(key);
        bucketLocks[bucket].writeLock().lock();
        myMap.put(key,value);
        if (backupAddress != null) {
            try{
                if (clientPool[bucket] == null) {
                    TSocket sock = new TSocket(backupAddress.getHostName(), backupAddress.getPort());
                    TTransport transport = new TFramedTransport(sock);
                    transport.open();
                    TProtocol protocol = new TBinaryProtocol(transport);
                    KeyValueService.Client client = new KeyValueService.Client(protocol);
                    clientPool[bucket] = client;
                }
                clientPool[bucket].backupPut(key, value);
            } catch (Exception e) {
                System.out.println("Failed to write to backup");
                System.out.println(e.getLocalizedMessage());
            }
        } else {
            System.out.println("backup is null");
        }
        bucketLocks[bucket].writeLock().unlock();
    }


    public void backupPut(String key, String value) throws org.apache.thrift.TException {
        // System.out.println("backupPut with key: " + key + " value: " + value);
        myMap.put(key,value);
    }

    public void sync(Map<String, String> primaryMap) throws org.apache.thrift.TException {
        myMap = new ConcurrentHashMap(primaryMap);
    }

    public void copyTableInReplica(String host, int port) {
        for(int i = 0; i < bucketLocks.length; i++) {
            bucketLocks[i].readLock().lock();
        }
        try {
            TSocket sock = new TSocket(host, port);
            TTransport transport = new TFramedTransport(sock);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            KeyValueService.Client client = new KeyValueService.Client(protocol);
            client.sync(myMap);
            transport.close();
        } catch(Exception e) {
            System.out.println("Failed to copy to replica");
            System.out.println(e.getLocalizedMessage());
        }
        for(int i = 0; i < bucketLocks.length; i++) {
            bucketLocks[i].readLock().unlock();
        }
    }

    synchronized public void determineNodes() throws Exception {
        List<String> children;
        while(true) {
            curClient.sync();
            children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
            Collections.sort(children);
            if (children.size() == 1) {
                primaryAddress = new InetSocketAddress(host, port);
                backupAddress = null;
                clientPool = null;
            } else if (children.size() > 1) {
                byte[] backupData = curClient.getData().forPath(zkNode + "/" + children.get(1));
                String backupStrData = new String(backupData);
                String[] backup = backupStrData.split(":");
                if (isPrimary()) {
                    System.out.println("Confirm only the primary runs this");
                    copyTableInReplica(backup[0], Integer.parseInt(backup[1]));
                    clientPool = new KeyValueService.Client[62];
                }
                backupAddress = new InetSocketAddress(backup[0], Integer.parseInt(backup[1]));
                System.out.println("Confirm this runs after syncing");
                byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
                String strData = new String(data);
                String[] primary = strData.split(":");
                primaryAddress = new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
                System.out.println("Found primary " + strData);
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

    private synchronized Boolean isPrimary() {
        if (null == primaryAddress) return false;
        return (host.equals(primaryAddress.getHostName()) && port == primaryAddress.getPort());
    }
}
