package br.ufmg.cs.systems.fractal.gmlib.clique;

import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.*;
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
import java.util.*;

public class KClistEnumerator<S extends Subgraph> extends SubgraphEnumerator<S> implements Serializable {

    // TODO SynchronizedDynamicGraphV3 dag;
    // current clique DAG
    transient private IntObjMap<IntArrayList> dag;
    // used to clear the dag
    transient private DagCleaner dagCleaner;
    public static int count = 0;
    public static int size = 1;
    public static boolean writeSizes = false;
    public static LightBasicMainGraph<Vertex<Integer>, Edge<Integer>> graph = null;

    private static List<Integer> colors = null;
    //private static List<Integer> neigboursColorsCount = null;

    public static List<Integer> getColors() {
        return colors;
    }

    public static void setColors(Integer[] colors0) {
        colors = new ArrayList<>(Arrays.asList(colors0));
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

    public KClistEnumerator() {}

    @Override
    public void init(Configuration<S> config) {
        dag = HashIntObjMaps.newMutableMap();
        dagCleaner = new DagCleaner();
    }

    @Override
    //TODO: WHY DO I NEED THIS????
    public void rebuildState() {
        dag.clear();
//        if (subgraph.getNumWords() == 0) return;
//        IntArrayList vertices = subgraph.getVertices();
//        IntObjMap<IntArrayList> currentDag = HashIntObjMaps.newMutableMap();
//        IntObjMap<IntArrayList> aux;
//
//        //extendFromGraph(computation.getConfig().getMainGraph(), dag, vertices.get(0));
//
//        for (int i = 1; i < vertices.size(); ++i) {
//            aux = currentDag;
//            currentDag = dag;
//            dag = aux;
//            dag.clear();
//            extendFromDag(computation.getConfig().getMainGraph(), currentDag, dag, vertices.get(i));
//        }
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
            extendFromGraph(nextEnumerator.dag, u);
        } else {
            extendFromDag(dag, nextEnumerator.dag, u);
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
    public void extendFromDag(IntObjMap<IntArrayList> currentDag,
                              IntObjMap<IntArrayList> dag, int u) {

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
            Utils.smartChoiceIntersect(orderedVertices, orderedVertices2,
                    i + 1, orderedVertices.size(),
                    0, orderedVertices2.size(), target);
            dag.put(v, target);
        }
    }

    /**
     * Extend this enumerator from the input graph (bootstrap)
     *
     * @param u first vertex being added to the current subgraph
     */
    private static void extendFromGraph(IntObjMap<IntArrayList> dag, int u) {
        count++;

        //VertexNeighbourhood neighborhood = graph.getVertexNeighbourhood(u);

        List<Integer> neighborhood = graph.getVertexNeighboursList(u);

        if (neighborhood.size() == 0) {
            return;
        }

        //IntArrayList orderedVertices = neighborhood.getOrderedVertices();

        dag.ensureCapacity(neighborhood.size());

        for (int i = 0; i < neighborhood.size(); ++i) {
            //int v = orderedVertices.getUnchecked(i);
            int v = neighborhood.get(i);
            dag.put(v, IntArrayListPool.instance().createObject());
        }

        if (writeSizes) {
            //System.out.println("neighborhood.size: " + orderedVertices.size());
            System.out.println("neighborhood.size: " + neighborhood.size());
        }

        long time = System.currentTimeMillis();
        long lens = 0;
        IntObjCursor<IntArrayList> cur = dag.cursor();
        while (cur.moveNext()) {
            int v = cur.key();
            neighborhood = graph.getVertexNeighboursList(v);

            if (neighborhood.size() == 0) {
                continue;
            }

            //IntArrayList orderedVertices2 = neighborhood.getOrderedVertices();
            lens += neighborhood.size();
            for (int j = 0; j < neighborhood.size(); ++j) {
                //int w = orderedVertices2.getUnchecked(j);
                int w = neighborhood.get(j);
                if (dag.containsKey(w)) {
                    cur.value().add(w);
                }
            }
        }

        if (writeSizes) {
            System.out.println("cur.value().add: " + (System.currentTimeMillis() - time) / 1000.0 + "s");

            System.out.println("lens: " + lens);
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

    // Function to assign colors to vertices of graph
    private static List<Integer> colorGraph(MainGraph graph1) {
        System.out.println("STARRT COLORING");
        long time = System.currentTimeMillis();
        int N = graph1.getNumberVertices();
        List<GraphInner.Edge1> edges = new ArrayList<>();
        br.ufmg.cs.systems.fractal.graph.Edge[] edges1 = graph1.getEdges();
        int E = graph1.getNumberEdges();
        for (int i = 0; i < E; i++) {
            edges.add(new GraphInner.Edge1(edges1[i].getSourceId(), edges1[i].getDestinationId()));
        }
        GraphInner graph = new GraphInner(edges, N);
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
//        neigboursColorsCount = new ArrayList<>(Collections.nCopies(N, 0));
//        for (int u = 0; u < N; u++) {
//            Set<Integer> assigned = new TreeSet<>();
//            for (int i : graph.adjList.get(u)) {
//                assigned.add(result.get(i));
//            }
//            neigboursColorsCount.set(u, assigned.size());
//        }


        System.out.println("Coloring: " + (System.currentTimeMillis() - time) / 1000.0 + "s");
        System.out.println(result);
        return result;
    }

    public static void countAndSetColors(LightBasicMainGraph<Vertex<Integer>, Edge<Integer>> graph) {
        System.out.println("Start coloring");
        long time = System.currentTimeMillis();

        colors = new ArrayList<>(Collections.nCopies(graph.N + 1, 0));

        for (Map.Entry<Integer, List<Integer>> entry : graph.getAdjList().entrySet()) {

            Set<Integer> assigned = new TreeSet<>();
            for (int i : entry.getValue()) {
                if (colors.get(i) != 0) {
                    assigned.add(colors.get(i));
                }
            }
            int color = 1;
            for (Integer c : assigned) {
                if (color != c) {
                    break;
                }
                color++;
            }
            colors.set(entry.getKey(), color);
        }

        System.out.println("Coloring: " + (System.currentTimeMillis() - time) / 1000.0 + "s");
    }
}
