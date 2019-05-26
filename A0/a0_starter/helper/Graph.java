package helper;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import helper.Edge;

class Struct {
    public int rootId;
    public int rank;
    public Struct(int rootId, int rank) {
        this.rootId = rootId;
        this.rank = rank;
    }
}

public class Graph {
    private HashMap<Integer, Struct> map;
    public Graph() {
        map = new HashMap<Integer, Struct>();
    }

    public void addEdge(Edge edge) {

        int srcId = edge.getSource();
        int destId = edge.getDestination();
        if(!map.containsKey(srcId)) map.put(srcId, new Struct(srcId,0));
        if(!map.containsKey(destId)) map.put(destId, new Struct(destId,0));
        union(srcId, destId);
    }

    public Struct findRoot(int vertexId) {
        int rootId = map.get(vertexId).rootId;
        if (rootId != vertexId) {
            Struct newRootId = findRoot(rootId);
            map.put(vertexId, newRootId);
        }
        return map.get(vertexId);
    }

    public void union(int x, int y) {
        Struct xRoot = findRoot(x);
        Struct yRoot = findRoot(y);
        if (xRoot.rootId != yRoot.rootId) {
            if (xRoot.rank < yRoot.rank) {
                map.put(xRoot.rootId, yRoot);
            } else if (xRoot.rank > yRoot.rank) {
                map.put(yRoot.rootId, xRoot);
            } else {
                xRoot.rank++;
                map.put(yRoot.rootId, xRoot);
            }
        }
    }

    public HashMap<Integer, Integer> connectedComponents() {
        HashMap<Integer,Integer> ret = new HashMap<Integer,Integer>();
        for (int vertex : map.keySet()) {
            ret.put(vertex,findRoot(vertex).rootId);
        }
        return ret;
    }
}
