package br.ufmg.cs.systems.fractal.gmlib.clique;

import br.ufmg.cs.systems.fractal.computation.SubgraphEnumerator;
import br.ufmg.cs.systems.fractal.conf.Configuration;
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
import com.twitter.cassovary.graph.node.SynchronizedDynamicNode;

import java.util.*;

public class KClistEnumerator<S extends Subgraph> extends SubgraphEnumerator<S> {

  // TODO SynchronizedDynamicGraphV3 dag;
  // current clique DAG
  private IntObjMap<IntArrayList> dag;
  // used to clear the dag
  private DagCleaner dagCleaner;
  public static int count = 0;
  public static long t = 0;

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

    extendFromGraph(computation.getConfig().getMainGraph(),
            dag, vertices.get(0));

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

  static boolean print = false;

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
  public void setForFrozen(IntArrayList prefix, IntObjMap<IntArrayList>  dag) {
    super.setForFrozen(prefix, dag);
    this.dag = dag;
  }

  /**
   * Extend this enumerator from a previous DAG
   * @param u vertex being added to the current subgraph
   */
  public void extendFromDag(MainGraph graph, IntObjMap<IntArrayList> currentDag,
                            IntObjMap<IntArrayList> dag, int u) {

    count++;
    if (print) {
      System.out.print("add " + graph.getVertex(u).getVertexOriginalId() + " to Set(");
      for (int i = 0; i < prefix.size(); i++) {
        if (i != 0) {
          System.out.print(", ");
        }
        System.out.print(graph.getVertex(prefix.getUnchecked(i)).getVertexOriginalId());
      }
      System.out.println(")");
    }
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
              //TODO
              //i + 1, orderedVertices.size(),
              0, orderedVertices.size(),
              0, orderedVertices2.size(), target);
      dag.put(v, target);
    }
  }

  /**
   * Extend this enumerator from the input graph (bootstrap)
   * @param u first vertex being added to the current subgraph
   */
  private static void extendFromGraph(MainGraph graph,
                                      IntObjMap<IntArrayList> dag, int u) {
    count++;


    if (print) {
      System.out.println("start with " + graph.getVertex(u).getVertexOriginalId());
    }
    VertexNeighbourhood neighborhood = graph.getVertexNeighbourhood(u);

    if (neighborhood == null) {
      return;
    }

    IntArrayList orderedVertices = neighborhood.getOrderedVertices();

    dag.ensureCapacity(orderedVertices.size());

    for (int i = 0; i < orderedVertices.size(); ++i) {
      int v = orderedVertices.getUnchecked(i);
      //TODO no need to check this?
      //if (v > u) {
        dag.put(v, IntArrayListPool.instance().createObject());
      //}
    }

    IntObjCursor<IntArrayList> cur = dag.cursor();
    while (cur.moveNext()) {
      int v = cur.key();
      neighborhood = graph.getVertexNeighbourhood(v);

      if (neighborhood == null) continue;

      IntArrayList orderedVertices2 = neighborhood.getOrderedVertices();

      for (int j = 0; j < orderedVertices2.size(); ++j) {
        int w = orderedVertices2.getUnchecked(j);
        //if (w > v && dag.containsKey(w)) {
        if (dag.containsKey(w)) {
          cur.value().add(w);
        }
      }
    }
  }


  @Override
  public IntObjMap<IntArrayList> getDag(){
    return dag;
  }

  private void clearDag() {
    dag.forEach(dagCleaner);
    dag.clear();
  }

  private class DagCleaner implements IntObjConsumer<IntArrayList> {
    @Override
    public void accept(int u, IntArrayList neighbors) {
      IntArrayListPool.instance().reclaimObject(neighbors);
    }
  }




  // data structure to store graph edges


  // class to represent a graph object
  private static  class Graph
  {
    static class Edge1
    {
      int source, dest;

      Edge1(int source, int dest) {
        this.source = source;
        this.dest = dest;
      }
    }
    // An array of Lists to represent adjacency list
    List<List<Integer>> adjList = null;

    // Constructor
    Graph(List<Edge1> edges, int N)
    {
      adjList = new ArrayList<>();
      for (int i = 0; i < N; i++) {
        adjList.add(new ArrayList<>());
      }

      // add edges to the undirected graph
      for (Edge1 edge: edges)
      {
        int src = edge.source;
        int dest = edge.dest;

        adjList.get(src).add(dest);
        adjList.get(dest).add(src);
      }
    }
  }
    // Function to assign colors to vertices of graph
    public static Map<Integer, Integer> colorGraph(MainGraph graph1) {
      long time = System.currentTimeMillis();
      int N = graph1.getNumberVertices();
      List<Graph.Edge1> edges = new ArrayList<>();
      br.ufmg.cs.systems.fractal.graph.Edge[] edges1 = graph1.getEdges();
      int E = graph1.getNumberEdges();
      for (int i = 0; i< E; i++) {
        edges.add(new Graph.Edge1(
                graph1.getVertex(edges1[i].getSourceId()).getVertexOriginalId(),
                graph1.getVertex(edges1[i].getDestinationId()).getVertexOriginalId()
        ));
      }
      Graph graph = new Graph(edges, N);
      Map<Integer, Integer> result = new HashMap<>();
      for (int u = 0; u < N; u++) {
        Set<Integer> assigned = new TreeSet<>();
        for (int i : graph.adjList.get(u)) {
          if (result.containsKey(i)) {
            assigned.add(result.get(i));
          }
        }
        int color = 1;
        for (Integer c: assigned) {
          if (color != c) {
            break;
          }
          color++;
        }
        result.put(u, color);
      }
      System.out.println("Coloring: " + (System.currentTimeMillis() - time) / 1000.0 + "s");
      return result;
  }
}
