import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;

class Edge {
    private int source;
    private int destination;

    public Edge(int source, int destination) {
        this.source = source;
        this.destination = destination;
    }

    public int getSource() {
        return this.source;
    }

    public int getDestination() {
        return this.destination;
    }
}

class Subset {
    private int parent;
    private int rank;

    public Subset(int parent, int rank) {
        this.parent = parent;
        this.rank = rank;
    }

    public int getRank() {
        return this.rank;
    }

    public void incRank() {
        this.rank++;
    }

    public int getParent() {
        return this.parent;
    }

    public void setParent(int parent) {
        this.parent = parent;
    }
}

class Graph {
    private ArrayList<Edge> edges;
    private HashMap<Integer,Subset> subsets;

    public Graph() {
        edges = new ArrayList<Edge>();
        subsets = new HashMap<Integer,Subset>();
    }

    public void addEdge(Edge edge) {
        edges.add(edge);

        // initialize subsets with edge vertices, set each vertex to be its own parent with rank 0
        if (!subsets.containsKey(edge.getSource())){
            Subset subset = new Subset(edge.getSource(), 0);
            subsets.put(edge.getSource(), subset);
        }
        if (!subsets.containsKey(edge.getDestination())) {
            Subset subset = new Subset(edge.getDestination(), 0);
            subsets.put(edge.getDestination(), subset);
        }
    }

    // find parent of vertex (path compression done here)
    public int findRoot(int vertexId) {
        int parentId = subsets.get(vertexId).getParent();
        if (parentId != vertexId) {
            subsets.get(vertexId).setParent(findRoot(parentId));
        }
        return subsets.get(vertexId).getParent();
    }

    // perform union by union by rank
    public void union(int x, int y) {
        int xRoot = findRoot(x);
        int yRoot = findRoot(y);
        int xRootRank = subsets.get(xRoot).getRank();
        int yRootRank = subsets.get(yRoot).getRank();
        if (xRootRank < yRootRank) {
            subsets.get(xRoot).setParent(yRoot);
        } else if (yRootRank < xRootRank) {
            subsets.get(yRoot).setParent(xRoot);
        }
        else {
            subsets.get(yRoot).setParent(xRoot);
            xRootRank++;
            subsets.get(xRoot).incRank();
        }
    }

    // return connected components
    public void connectedComponents() {
        for (Edge edge : edges) {
            int srcVertex = edge.getSource();
            int destVertex = edge.getDestination();
            union(srcVertex, destVertex);
        }
        for (int vertex : subsets.keySet()) {
            System.out.println("vertex: " + vertex + " root: " + subsets.get(vertex).getParent());
        }
    }

    public static void main(String args[]) {
        String filename =
        Edge edge1 = new Edge(1,2);
        Edge edge2 = new Edge(3,5);
        Edge edge3 = new Edge(4,5);
        Edge edge4 = new Edge(3,4);

        Graph g = new Graph();
        g.addEdge(edge1);
        g.addEdge(edge2);
        g.addEdge(edge3);
        g.addEdge(edge4);
        g.connectedComponents();
    }
}
