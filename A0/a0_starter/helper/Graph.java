package helper;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;

public class Graph {
    private ArrayList<Edge> edges;
    private HashMap<Integer, Integer> ranks;
    private HashMap<Integer, Integer> roots;
    private HashMap<Integer, ArrayList<Integer>> subsets;

    private void updateSet(int setId, int addedSetId) {
        // set the root of all elements in the set to be added to be newRootId
        ArrayList<Integer> arr = subsets.get(addedSetId);
        for (int vertex : arr) {
            roots.replace(vertex, setId);
        }
        subsets.get(setId).add(addedSetId);
        subsets.get(setId).addAll(subsets.get(addedSetId));
    }

    public Graph() {
        edges = new ArrayList<Edge>();
        ranks = new HashMap<Integer,Integer>();
        roots = new HashMap<Integer,Integer>();
        subsets = new HashMap<Integer, ArrayList<Integer>>();
    }

    public void addEdge(Edge edge) {
        edges.add(edge);
        int srcId = edge.getSource();
        int destId = edge.getDestination();
        if (!ranks.containsKey(srcId)) ranks.put(srcId, 0);
        if (!ranks.containsKey(destId)) ranks.put(destId, 0);
        if (!roots.containsKey(srcId)) roots.put(srcId, srcId);
        if (!roots.containsKey(destId)) roots.put(destId, destId);
        if (!subsets.containsKey(srcId)) {
            ArrayList<Integer> arr = new ArrayList<Integer>();
            subsets.put(srcId,arr);
        }
        if (!subsets.containsKey(destId)) {
            ArrayList<Integer> arr = new ArrayList<Integer>();
            subsets.put(destId, arr);
        }
    }

    // find parent of vertex (path compression done here)
    public int findRoot(int vertexId) {
        int parentId = roots.get(vertexId);
        if (parentId != vertexId) {
            int newRootId = findRoot(parentId);
            roots.replace(vertexId,newRootId);
        }
        return roots.get(vertexId);
    }

    // perform union by union by rank
    public void union(int x, int y) {
        int xRoot = findRoot(x);
        int yRoot = findRoot(y);
        if (xRoot != yRoot) {
            int xRootRank = ranks.get(xRoot);
            int yRootRank = ranks.get(yRoot);
            if (xRootRank < yRootRank) {
                roots.replace(xRoot,yRoot);
                updateSet(yRoot,xRoot);
            } else if (yRootRank < xRootRank) {
                roots.replace(yRoot,xRoot);
                updateSet(xRoot,yRoot);
            }
            else {
                roots.replace(yRoot,xRoot);
                ranks.replace(xRoot,ranks.get(xRoot) + 1);
                updateSet(xRoot,yRoot);
            }
        }
    }

    // return connected components
    public HashMap<Integer,Integer> connectedComponents() {
        for (Edge edge : edges) {
            int srcVertex = edge.getSource();
            int destVertex = edge.getDestination();
            union(srcVertex, destVertex);
        }
        return roots;
    }
}
