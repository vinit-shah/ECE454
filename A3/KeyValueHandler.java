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
    public KeyValueHandler(String myHost, int myPort, CuratorFramework curClient, String zkNode) {
    	this.myHost = myHost;
    	this.myPort = myPort;
    	this.curClient = curClient;
    	this.zkNode = zkNode;
    	myMap = new ConcurrentHashMap<String, String>();
        rwLock = new ReentrantReadWriteLock();
    }

    public String get(String key) throws org.apache.thrift.TException
    {
        rwLock.readLock().lock();
    	String ret = myMap.get(key);
    	if (ret == null) {
            rwLock.readLock().unlock();
    	    return "";
        }
    	else {
            rwLock.readLock().unlock();
    	    return ret;
        }
    }

    public void put(String key, String value) throws org.apache.thrift.TException
    {
        if (isPrimary) {
            rwLock.writeLock().lock();

        } else {
            myMap.put(key,value);
        }
    }

    public void updatePrimary(String hostName, int portNumber){
        // System.out.println("Primary: " + hostName + ":" + portNumber);
        primaryHost = hostName;
        primaryPort = portNumber;
        isPrimary = (myHost.equals(primaryHost) &&  myPort == primaryPort);
    }

    public void updateBackup(String hostName, int portNumber) {

    }

    private getThriftClientToBackup() {
        while(true) {

        }
    }
}
