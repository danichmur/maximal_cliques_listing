package br.ufmg.cs.systems.fractal.gmlib.clique;

import java.util.ArrayList;
import java.util.List;

public class GraphInner {
    public static class Edge1 {
        public int source, dest;

        public Edge1(int source, int dest) {
            //if (source < dest) {
                this.source = source;
                this.dest = dest;
//            } else {
//                this.source = dest;
//                this.dest = source;
//            }
        }
    }

    List<List<Integer>> adjList = null;

    GraphInner(List<Edge1> edges, int N) {
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
}