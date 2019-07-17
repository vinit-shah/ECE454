import java.util.*;
import java.util.concurrent.*;
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
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    volatile InetSocketAddress primaryAddress;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
    	this.host = host;
    	this.port = port;
    	this.curClient = curClient;
    	this.zkNode = zkNode;
    	myMap = new ConcurrentHashMap<String, String>();
        primaryAddress = null;
    }

    public String get(String key) throws org.apache.thrift.TException
    {
    	String ret = myMap.get(key);
    	if (ret == null)
    	    return "";
    	else
    	    return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException
    {
    	myMap.put(key, value);
    }

    private InetSocketAddress getPrimary() throws Exception {
    	while (true) {
    	    curClient.sync();
    	    List<String> children =
    		curClient.getChildren().usingWatcher(this).forPath(zkNode);
    	    if (children.size() == 0) {
    		// log.error("No primary found");
    		Thread.sleep(100);
    		continue;
    	    }
    	    Collections.sort(children);
    	    byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
    	    String strData = new String(data);
    	    String[] primary = strData.split(":");
    	    // log.info("Found primary " + strData);
    	    return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
    	}
    }

    synchronized public void process(WatchedEvent event) {
    	try {
    	    primaryAddress = getPrimary();
    	} catch (Exception e) {
    	}

    }
}
