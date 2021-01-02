package br.ufmg.cs.systems.fractal.gmlib.clique;

import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.graph.MainGraph;
import br.ufmg.cs.systems.fractal.graph.Vertex;
import br.ufmg.cs.systems.fractal.graph.VertexNeighbourhood;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.Utils;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntSet;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;
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

    private static List<Integer> colors = null;
    private static List<Integer> neigboursColorsCount = null;

    public static List<Integer> getColors(MainGraph graph) {
        if (colors == null) {
            colorGraph(graph);
        }
        return colors;
    }

    public static void setColors(Integer[] colors0) {
        colors = new ArrayList<>(Arrays.asList(colors0));
    }

    //This guys serialize only dags
    private void writeObject(ObjectOutputStream out) throws IOException {
        //out.defaultWriteObject();

        IntObjCursor<IntArrayList> dagCur = dag.cursor();
        int i = 0;
//        while (dagCur.moveNext()) {
//            if (dagCur.key() == cur.elem()) {
//                break;
//            }
//            i++;
//        }

        out.writeInt(dag.size() - i);
        out.writeInt(currElem);


        while (dagCur.moveNext()) {
            out.writeInt(dagCur.key());
            out.writeObject(dagCur.value());
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        //in.defaultReadObject();

        init(null);
        int dagSize = in.readInt();
        currElem = in.readInt();
        dag.ensureCapacity(dagSize);

        while (dagSize != 0) {
            dagSize--;
            int key = in.readInt();
            IntArrayList values = (IntArrayList) in.readObject();
            dag.put(key, values);
        }
        //set(dag.keySet());
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
    public void setForFrozen(S subgraph, IntObjMap<IntArrayList> dag) {
        super.setForFrozen(subgraph, dag);
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
                    //TODO: i+1 ~ seems, we have to sort :(
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

        IntSet orderedVertices = graph.getVertexNeighbours(u);

        dag.ensureCapacity(orderedVertices.size());
        orderedVertices.getInternalSet().toIntArray();
        IntCursor cur0 = orderedVertices.getInternalSet().cursor();
        while (cur0.moveNext()) {
            int v = cur0.elem();
            if (neigboursColorsCount.get(v) >= size - 1) {
                dag.put(v, IntArrayListPool.instance().createObject());
            }
        }

        if (writeSizes) {
            System.out.println("neighborhood.size: " + orderedVertices.size() + " dag.size: " + dag.keySet().size());
        }

        long time = System.currentTimeMillis();
        long lens = 0;
        IntObjCursor<IntArrayList> cur = dag.cursor();
        while (cur.moveNext()) {
            int v = cur.key();

            IntCollection orderedVertices2 = graph.getVertexNeighbours(v).getInternalSet();
            if (orderedVertices2 == null) continue;

            lens += orderedVertices2.size();
            IntCursor cur2 = orderedVertices2.cursor();
            while (cur2.moveNext()) {
                if (neigboursColorsCount.get(v) >= size - 1 && dag.containsKey(cur2.elem())) {
                    cur.value().add(cur2.elem());
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
    private static void colorGraph(MainGraph graph) {
        int N = graph.getNumberVertices() + 1;
        System.out.println("Start coloring " + N);
        long time = System.currentTimeMillis();

        colors = new ArrayList<>(Collections.nCopies(N, 0));

        for (int u = 0; u < N; u++) {
            Set<Integer> assigned = new TreeSet<>();
            IntSet neigh = graph.getVertexNeighbours(u);
            if (neigh == null) {
                continue;
            }
            System.out.println(u + " " + neigh.getInternalSet());// + " " + graph.getReversedVertexNeighbours(u).getInternalSet());

            for (int i : neigh.getInternalSet()) {
                if (colors.get(i) != 0) {
                    assigned.add(colors.get(i));
                }
            }

            for (int i : graph.getReversedVertexNeighbours(u).getInternalSet()) {
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
            colors.set(u, color);
        }

        //System.out.println("colored " + (System.currentTimeMillis() - time) / 1000.0 + "s");
        //long time1 = System.currentTimeMillis();
        //TODO
        neigboursColorsCount = new ArrayList<>(Collections.nCopies(N, N));

//        for (int u = 0; u < N; u++) {
//            Set<Integer> assigned = new TreeSet<>();
//            for (int i : graph.getVertexNeighbours(u).getInternalSet()) {
//                if (colors.get(i) != 0) {
//                    assigned.add(colors.get(i));
//                }
//            }
//            for (int i : graph.getReversedVertexNeighbours(u).getInternalSet()) {
//                if (colors.get(i) != 0) {
//                    assigned.add(colors.get(i));
//                }
//            }
//            neigboursColorsCount.set(u, assigned.size());
//        }

//        System.out.println("neigboursColorsCount " + (System.currentTimeMillis() - time1) / 1000.0 + "s");

//        for (int u = 1; u < N; u++) {
//            graph.removeLowers(u);
//        }
        graph.cleanReversedNeighbourhood();

        System.out.println("Coloring: " + (System.currentTimeMillis() - time) / 1000.0 + "s");
        System.out.println(colors);
    }

    public static void countAndSetColors(GraphInner graph) {
        System.out.println("Start coloring");
        long time = System.currentTimeMillis();

        colors = new ArrayList<>(Collections.nCopies(graph.N + 1, 0));

        for (Map.Entry<Integer, List<Integer>> entry : graph.adjListNew.entrySet()) {
            if (entry.getKey() % 1000 == 0) {
              //  System.out.println(entry.getKey() + " " + (System.currentTimeMillis() - time) / 1000.0 + "s");
            }

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

        neigboursColorsCount = new ArrayList<>(Collections.nCopies(graph.N + 1, 0));
        for (int u = 0; u < graph.N + 1; u++) {
            Set<Integer> assigned = new TreeSet<>();
            for (int i : graph.adjListNew.get(u)) {
                assigned.add(colors.get(i));
            }
            neigboursColorsCount.set(u, assigned.size());
        }

        System.out.println("Coloring: " + (System.currentTimeMillis() - time) / 1000.0 + "s");
        System.out.println(colors);
        colors = null;
    }
}
