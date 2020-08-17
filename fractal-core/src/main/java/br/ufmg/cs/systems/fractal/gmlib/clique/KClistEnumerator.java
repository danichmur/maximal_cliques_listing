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

public class KClistEnumerator<S extends Subgraph> extends SubgraphEnumerator<S> {

  // TODO SynchronizedDynamicGraphV3 dag;
  // current clique DAG
  private IntObjMap<IntArrayList> dag;

  // used to clear the dag
  private DagCleaner dagCleaner;
  public static int count = 0;
  public KClistEnumerator() {
  }

  @Override
  public void init(Configuration<S> config) {
    dag = HashIntObjMaps.newMutableMap();
    dagCleaner = new DagCleaner();
  }

  @Override
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
    count++;
    //long time = System.nanoTime();
    KClistEnumerator<S> nextEnumerator = (KClistEnumerator<S>) computation.
            nextComputation().getSubgraphEnumerator();


    //System.out.println("EXTEND: " + u + " " + prefix.toString());
    nextEnumerator.clearDag();

    if (subgraph.getNumWords() == 0) {
      extendFromGraph(subgraph.getConfig().getMainGraph(), nextEnumerator.dag, u);
    } else {
      extendFromDag(dag, nextEnumerator.dag, u);
    }


    subgraph.addWord(u);
    shouldRemoveLastWord = true;

//    time = (System.nanoTime() - time);
//    System.out.println("extend " + time / 1e9);

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
  public static void extendFromDag(IntObjMap<IntArrayList> currentDag,
                                   IntObjMap<IntArrayList> dag, int u) {
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
              i + 1, orderedVertices.size(),
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
    VertexNeighbourhood neighborhood = graph.getVertexNeighbourhood(u);

    if (neighborhood == null) {
      return;
    }

    IntArrayList orderedVertices = neighborhood.getOrderedVertices();

    dag.ensureCapacity(orderedVertices.size());

    for (int i = 0; i < orderedVertices.size(); ++i) {
      int v = orderedVertices.getUnchecked(i);
      if (v > u) {
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
        if (w > v && dag.containsKey(w)) {
          cur.value().add(w);
        }
      }
    }
  }


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

}
