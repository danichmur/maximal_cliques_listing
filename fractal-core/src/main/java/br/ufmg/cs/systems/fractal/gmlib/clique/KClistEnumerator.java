package br.ufmg.cs.systems.fractal.gmlib.clique;

import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.Edge;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.graph.VertexNeighbourhood;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.Utils;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.map.IntObjCursor;
import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.function.IntObjConsumer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;

public class KClistEnumerator<S extends Subgraph> extends SubgraphEnumerator<S> implements Serializable {

    // TODO SynchronizedDynamicGraphV3 dag;
    // current clique DAG
    transient private IntObjMap<IntArrayList> dag;
    // used to clear the dag
    transient private DagCleaner dagCleaner;
    public static int count = 0;
    public static long t = 0;
    public static int size = 1;

    private static List<Integer> colors = null;
    private static List<Integer> neigboursColorsCount = null;

    public static List<Integer> getColors(MainGraph graph) {
        if (colors == null) {
            colors = colorGraph(graph);
        }
        return colors;
    }

    public static void setColors(Integer[] colors0) {
        colors = new ArrayList<>(Arrays.asList(colors0));
    }

    public static Integer[] getColors2(Edge<Integer>[] edges1) {
        System.out.println("STARRT COLORING");
        long time = System.currentTimeMillis();
        List<Graph.Edge1> edges = new ArrayList<>();
        int N = 0;
        for (br.ufmg.cs.systems.fractal.graph.Edge<Integer> integerEdge : edges1) {
            int e1 = integerEdge.getSourceId();
            int e2 = integerEdge.getDestinationId();
            if (e1 > N) N = e1;
            if (e2 > N) N = e2;
            edges.add(new Graph.Edge1(e1, e2));
        }
        N++;
        Graph graph = new Graph(edges, N);
        List<Integer> result = new ArrayList<>(Collections.nCopies(N, 0));
        for (int u = 0; u < N; u++) {
            Set<Integer> assigned = new TreeSet<>();
            for (int i : graph.adjList.get(u)) {
                if (result.get(i) != 0) {
                    assigned.add(result.get(i));
                }
            }
            int color = 1;
            for (Integer c : assigned) {
                if (color != c) {
                    break;
                }
                color++;
            }
            result.set(u, color);
        }
        neigboursColorsCount = new ArrayList<>(Collections.nCopies(N, 0));
        for (int u = 0; u < N; u++) {
            Set<Integer> assigned = new TreeSet<>();
            for (int i : graph.adjList.get(u)) {
                assigned.add(result.get(i));
            }
            neigboursColorsCount.set(u, assigned.size());
        }


        System.out.println("Coloring: " + (System.currentTimeMillis() - time) / 1000.0 + "s");
        Integer[] array = new Integer[result.size()];
        result.toArray(array);
        return array;
    }



    //This guys serialize only dags
    private void writeObject(ObjectOutputStream out) throws IOException {
        //out.defaultWriteObject();

        IntObjCursor<IntArrayList> cur = dag.cursor();
        out.writeInt(dag.size());
        while (cur.moveNext()) {
            out.writeInt(cur.key());
            out.writeObject(cur.value());
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        //in.defaultReadObject();

        init(null);
        int dagSize = in.readInt();
        dag.ensureCapacity(dagSize);

        while (dagSize != 0) {
            dagSize--;
            int key = in.readInt();
            IntArrayList values = (IntArrayList) in.readObject();
            dag.put(key, values);
        }
    }

    public KClistEnumerator() {
    }

    @Override
    public void init(Configuration<S> config) {
        dag = HashIntObjMaps.newMutableMap();
        dagCleaner = new DagCleaner();
    }

    @Override
    //TODO: WHY DO I NEED THIS????
    public void rebuildState() {
        dag.clear();
        if (subgraph.getNumWords() == 0) return;
        IntArrayList vertices = subgraph.getVertices();
        IntObjMap<IntArrayList> currentDag = HashIntObjMaps.newMutableMap();
        IntObjMap<IntArrayList> aux;

        extendFromGraph(computation.getConfig().getMainGraph(), dag, vertices.get(0));

        for (int i = 1; i < vertices.size(); ++i) {
            aux = currentDag;
            currentDag = dag;
            dag = aux;
            dag.clear();
            extendFromDag(computation.getConfig().getMainGraph(), currentDag, dag, vertices.get(i));
        }
    }

    @Override
    public int getAdditionalSize() {
        return dag.size();
    }

    @Override
    public void computeExtensions() {
        if (subgraph.getNumWords() > 0) {
            set(dag.keySet());
        } else {
            super.computeExtensions();
        }
    }

    @Override
    public SubgraphEnumerator<S> extend(int u) {

        KClistEnumerator<S> nextEnumerator = (KClistEnumerator<S>) computation.nextComputation().getSubgraphEnumerator();
        nextEnumerator.clearDag();

        if (subgraph.getNumWords() == 0) {
            extendFromGraph(subgraph.getConfig().getMainGraph(), nextEnumerator.dag, u);
        } else {
            extendFromDag(subgraph.getConfig().getMainGraph(), dag, nextEnumerator.dag, u);
        }


        subgraph.addWord(u);
        shouldRemoveLastWord = true;

        return nextEnumerator;
    }

    @Override
    public void setForFrozen(IntArrayList prefix, IntObjMap<IntArrayList> dag) {
        super.setForFrozen(prefix, dag);
        this.dag = dag;
    }

    @Override
    public void setForFrozen(IntObjMap<IntArrayList> dag) {
        this.dag = dag;
    }

    /**
     * Extend this enumerator from a previous DAG
     *
     * @param u vertex being added to the current subgraph
     */
    public void extendFromDag(MainGraph graph, IntObjMap<IntArrayList> currentDag,
                              IntObjMap<IntArrayList> dag, int u) {

        getColors(graph);

        count++;

        IntArrayList orderedVertices = currentDag.get(u);

        if (orderedVertices == null) {
            return;
        }

        dag.ensureCapacity(orderedVertices.size());

        for (int i = 0; i < orderedVertices.size(); ++i) {
            int v = orderedVertices.getUnchecked(i);
            IntArrayList orderedVertices2 = currentDag.get(v);
            IntArrayList target = IntArrayListPool.instance().createObject();
            if (orderedVertices2 == null) {
                continue;
            }
            Utils.sintersect(orderedVertices, orderedVertices2,
                    0, orderedVertices.size(),
                    0, orderedVertices2.size(), target);
            dag.put(v, target);
        }
    }

    /**
     * Extend this enumerator from the input graph (bootstrap)
     *
     * @param u first vertex being added to the current subgraph
     */
    private static void extendFromGraph(MainGraph graph, IntObjMap<IntArrayList> dag, int u) {
        count++;

        getColors(graph);

        VertexNeighbourhood neighborhood = graph.getVertexNeighbourhood(u);

        if (neighborhood == null) {
            return;
        }

        IntArrayList orderedVertices = neighborhood.getOrderedVertices();

        dag.ensureCapacity(orderedVertices.size());

        for (int i = 0; i < orderedVertices.size(); ++i) {
            int v = orderedVertices.getUnchecked(i);
            if (neigboursColorsCount.get(v) >= size) {
                dag.put(v, IntArrayListPool.instance().createObject());
            }
        }

        IntObjCursor<IntArrayList> cur = dag.cursor();
        while (cur.moveNext()) {
            int v = cur.key();
            neighborhood = graph.getVertexNeighbourhood(v);

            if (neighborhood == null) continue;

            IntArrayList orderedVertices2 = neighborhood.getOrderedVertices();

            for (int j = 0; j < orderedVertices2.size(); ++j) {
                int w = orderedVertices2.getUnchecked(j);
                if (neigboursColorsCount.get(v) >= size && dag.containsKey(w)) {
                    cur.value().add(w);
                }
            }
        }
    }

    @Override
    public IntObjMap<IntArrayList> getDag() {
        return dag;
    }

    private void clearDag() {
        dag.forEach(dagCleaner);
        dag.clear();
    }

    private static class DagCleaner implements IntObjConsumer<IntArrayList> {
        @Override
        public void accept(int u, IntArrayList neighbors) {
            IntArrayListPool.instance().reclaimObject(neighbors);
        }
    }


    private static class Graph {
        static class Edge1 {
            int source, dest;

            Edge1(int source, int dest) {
                this.source = source;
                this.dest = dest;
            }
        }

        List<List<Integer>> adjList = null;

        Graph(List<Edge1> edges, int N) {
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

    // Function to assign colors to vertices of graph
    private static List<Integer> colorGraph(MainGraph graph1) {
        System.out.println("STARRT COLORING");
        long time = System.currentTimeMillis();
        int N = graph1.getNumberVertices();
        List<Graph.Edge1> edges = new ArrayList<>();
        br.ufmg.cs.systems.fractal.graph.Edge[] edges1 = graph1.getEdges();
        int E = graph1.getNumberEdges();
        for (int i = 0; i < E; i++) {
            edges.add(new Graph.Edge1(edges1[i].getSourceId(), edges1[i].getDestinationId()));
        }
        Graph graph = new Graph(edges, N);
        List<Integer> result = new ArrayList<>(Collections.nCopies(N, 0));
        for (int u = 0; u < N; u++) {
            Set<Integer> assigned = new TreeSet<>();
            for (int i : graph.adjList.get(u)) {
                if (result.get(i) != 0) {
                    assigned.add(result.get(i));
                }
            }
            int color = 1;
            for (Integer c : assigned) {
                if (color != c) {
                    break;
                }
                color++;
            }
            result.set(u, color);
        }
        neigboursColorsCount = new ArrayList<>(Collections.nCopies(N, 0));
        for (int u = 0; u < N; u++) {
            Set<Integer> assigned = new TreeSet<>();
            for (int i : graph.adjList.get(u)) {
                assigned.add(result.get(i));
            }
            neigboursColorsCount.set(u, assigned.size());
        }


        System.out.println("Coloring: " + (System.currentTimeMillis() - time) / 1000.0 + "s");
        System.out.println(result);
        return result;
    }
}
