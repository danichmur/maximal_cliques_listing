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
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.*;
import java.sql.Array;
import java.util.*;

public class KClistEnumerator<S extends Subgraph> extends SubgraphEnumerator<S> implements Serializable {

    // TODO SynchronizedDynamicGraphV3 dag;
    // current clique DAG
    transient private IntObjMap<IntArrayList> dag;
    // used to clear the dag
    transient private DagCleaner dagCleaner;
    public static int count = 0;
    public static int size = 1;

    public static int dumps = 1;
    public static int loads = 1;

    public static int EXTENDS_THRESHOLD = 106;

    private static ChronicleMap<String, byte[]> iterStorage = null;

    private static List<Integer> colors = null;
    public static List<Integer> neigboursSizes = null;
    private static List<Integer> neigboursColorsCount = null;

    public static List<Integer> getColors(MainGraph graph) {
        if (colors == null) {
            colorGraph(graph);
        }
        return colors;
    }

    public static void dropColors() {
        colors = null;
    }

    public static ChronicleMap<String, byte[]> getIterStorage() {
        if (iterStorage == null) {
            initIterStorage();
        }
        return iterStorage;
    }

    public static void addIter(String s, Object arr) {
        byte[] b = (byte[]) arr;
        System.out.println(b.length);
        getIterStorage().put(s, b);
    }

    public static byte[] getIter(String s) {
        byte[] b = getIterStorage().get(s);
        getIterStorage().remove(s);
        return b;
    }

    //This guys (de)serialize only dags
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        IntObjCursor<IntArrayList> dagCur = dag.cursor();
        int i = 0;
        out.writeInt(dag.size() - i);

        while (dagCur.moveNext()) {
            out.writeInt(dagCur.key());
            out.writeObject(dagCur.value());
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
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
    public void rebuildState() {
        dag = HashIntObjMaps.newMutableMap();
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
            extendFromDag(currentDag, dag, vertices.get(i));
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
        KClistEnumerator<S> nextEnumerator = (KClistEnumerator<S>) computation.getSubgraphEnumerator();
        if (!extend) {
            subgraph.addWord(u);
            shouldRemoveLastWord = true;

            return nextEnumerator;
        }
        //nextEnumerator.clearDag();

        if (subgraph.getNumWords() == 0) {
            extendFromGraph(subgraph.getConfig().getMainGraph(), nextEnumerator.dag, u);
        } else {
            extendFromDag(dag, nextEnumerator.dag, u);
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
    private void extendFromDag(IntObjMap<IntArrayList> currentDag, IntObjMap<IntArrayList> dag, int u) {

        count++;

        IntArrayList orderedVertices = currentDag.get(u);

        if (orderedVertices == null) {
            return;
        }

        //dag.ensureCapacity(orderedVertices.size());
        Set<Integer> visited = new HashSet<>();

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
            visited.add(v);
            dag.put(v, target);
        }

        IntObjCursor<IntArrayList> cur = dag.cursor();
        while (cur.moveNext()) {
            if (!visited.contains(cur.key())) {
                cur.remove();
            }
        }
    }

    /**
     * Extend this enumerator from the input graph (bootstrap)
     *
     * @param u first vertex being added to the current subgraph
     */
    private static void extendFromGraph(MainGraph graph, IntObjMap<IntArrayList> dag, int u) {
        count++;

        IntArrayList orderedVertices = graph.getVertexNeighbours(u);
        //in case this is not first computation for this iter
        dag.clear();

        dag.ensureCapacity(orderedVertices.size());
        IntCursor cur0 = orderedVertices.cursor();

        while (cur0.moveNext()) {
            int v = cur0.elem();
            if (neigboursColorsCount.get(v) >= size - 1) {
                dag.put(v, IntArrayListPool.instance().createObject());
            }
        }

        long time = System.currentTimeMillis();
        long lens = 0;
        IntObjCursor<IntArrayList> cur = dag.cursor();
        while (cur.moveNext()) {
            int v = cur.key();

            IntCollection orderedVertices2 = graph.getVertexNeighbours(v);
            if (orderedVertices2 == null) continue;

            lens += orderedVertices2.size();
            IntCursor cur2 = orderedVertices2.cursor();
            while (cur2.moveNext()) {
                if (neigboursColorsCount.get(v) >= size - 1 && dag.containsKey(cur2.elem())) {
                    cur.value().add(cur2.elem());
                }
            }
        }

        boolean writeSizes = true;
        if (writeSizes) {
            System.out.println("neighborhood.size: " + orderedVertices.size() + "; dag.size: " + dag.keySet().size() +
                    "; time: "+ (System.currentTimeMillis() - time) / 1000.0 + "s; lens: " + lens);
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

    private static void initIterStorage() {
        byte[] v = new byte[100000000];
        for (int i = 0; i < 100000000; i++) {
            v[i] = 1;
        }

        try {
            iterStorage = ChronicleMapBuilder
                    .of(String.class, byte[].class)
                    .name("iter_storage")
                    .entries(1_000)
                    .averageKeySize(20)
                    .averageValue(v)
                    .createPersistedTo(new File("iter_storage.dat"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Function to assign colors to vertices of graph
    private static void colorGraph(MainGraph graph) {
        int N = graph.getNumberVertices() + 1;
        System.out.println("Start coloring " + N);
        long time = System.currentTimeMillis();

        colors = new ArrayList<>(Collections.nCopies(N, 0));
        List<Integer> idx = new ArrayList<>(N);
        List<Integer> degrees = new ArrayList<>(N);
        for (int u = 0; u < N; u++) {
            idx.add(u);
            degrees.add(-(graph.getVertexNeighbours(u).size() + graph.getReversedVertexNeighbours(u).size()));
        }
        idx.sort(Comparator.comparing(degrees::get));

        for (int u = 0; u < N; u++) {
            Set<Integer> assigned = new TreeSet<>();
            IntArrayList neigh = graph.getVertexNeighbours(idx.get(u));
            //System.out.println(idx.get(u) + ": " + neigh.toString() + " " + graph.getReversedVertexNeighbours(idx.get(u)).getInternalSet());

            if (neigh == null) {
                continue;
            }

            for (int i : neigh) {
                if (colors.get(i) != 0) {
                    assigned.add(colors.get(i));
                }
            }

            for (int i : graph.getReversedVertexNeighbours(idx.get(u)).getInternalSet()) {
                if (colors.get(i) != 0) {
                    assigned.add(colors.get(i));
                }
            }

            int color = 1;
            while (assigned.contains(color)) {
                color++;
            }
            colors.set(idx.get(u), color);
        }

        System.out.println("colored " + (System.currentTimeMillis() - time) / 1000.0 + "s");

        long time1 = System.currentTimeMillis();
        neigboursColorsCount = new ArrayList<>(Collections.nCopies(N, 0));

        for (int u = 0; u < N; u++) {
            Set<Integer> assigned = new TreeSet<>();
            for (int i : graph.getVertexNeighbours(idx.get(u))) {
                if (colors.get(i) != 0) {
                    assigned.add(colors.get(i));
                }
            }
            for (int i : graph.getReversedVertexNeighbours(idx.get(u)).getInternalSet()) {
                if (colors.get(i) != 0) {
                    assigned.add(colors.get(i));
                }
            }
            neigboursColorsCount.set(idx.get(u), assigned.size());
        }

        neigboursSizes = new ArrayList(new TreeSet<>(neigboursColorsCount));


        //TODO
        //graph.cleanReversedNeighbourhood();

        System.out.println("Coloring: " + (System.currentTimeMillis() - time) / 1000.0 + "s; " + "neigboursColorsCount " + (System.currentTimeMillis() - time1) / 1000.0 + "s");
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

    public static boolean isClique(IntObjMap<IntArrayList> d) {
        int n = d.keySet().size();
        int edges = 0;
        IntObjCursor<IntArrayList> c = d.cursor();
        while (c.moveNext()) {
            edges += c.value().size();
        }

        //Complete graph with n vertices has n * (n - 1) / 2 edge
        return n * (n - 1) / 2 == edges;
    }
}
