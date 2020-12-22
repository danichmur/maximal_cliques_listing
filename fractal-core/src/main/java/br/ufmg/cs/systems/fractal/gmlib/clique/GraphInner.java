package br.ufmg.cs.systems.fractal.gmlib.clique;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class GraphInner {
    int N;
    //TODO LONG!!!
    public static class Edge1 {
        int source, dest;

        public Edge1(int source, int dest) {
            this.source = source;
            this.dest = dest;
        }
    }

    List<List<Integer>> adjList = null;

    HashMap<Integer, List<Integer>> adjListNew = null;

    GraphInner(List<Edge1> edges, int N) {
        this.N = N;
        adjList = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            adjList.add(new ArrayList<>());
        }

        for (Edge1 edge : edges) {
            int src = edge.source;
            int dest = edge.dest;

            adjList.get(src).add(dest);
            adjList.get(dest).add(src);
        }
    }

    public GraphInner() {
        adjList = new ArrayList<>();
        adjListNew = new HashMap<>();
    }

    public void addEdge(Edge1 e) {

        if(!adjListNew.containsKey(e.source)) {
            if (e.source > N) N = e.source;
            adjListNew.put(e.source, new ArrayList<>());
        }
        adjListNew.get(e.source).add(e.dest);

        if(!adjListNew.containsKey(e.dest)) {
            if (e.dest > N) N = e.dest;
            adjListNew.put(e.dest, new ArrayList<>());
        }
        adjListNew.get(e.dest).add(e.source);
    }
}