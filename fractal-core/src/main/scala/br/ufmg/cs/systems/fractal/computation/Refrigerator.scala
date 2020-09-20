package br.ufmg.cs.systems.fractal.computation

import java.util
import java.util.HashSet

import br.ufmg.cs.systems.fractal.gmlib.clique.FrozenDataHolderOld
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.util.{SynchronizedDynamicGraphV2, SynchronizedNodeBuilder}
import com.twitter.cassovary.graph.node.SynchronizedDynamicNode
import net.openhft.chronicle.map.ChronicleMap

import scala.collection.immutable.HashMap
import scala.collection.{immutable, mutable}

object Refrigerator {
  var freeze : Boolean = false
  var current : FrozenDataHolder = _
  var sizesMap = new mutable.HashMap[Integer, mutable.HashSet[Int]]

  var frozenMap: ChronicleMap[Integer, Array[Byte]] = _
  var availableSizes = new mutable.TreeSet[Int]()
  private var lock = new AnyRef{}
  var counter : Int = 0
  var graphCounter : Int = 0


  var size : Int = 0
  var kcores : immutable.Map[Int, Int] = _

  var start : Long = 0

  val isVertexOk: (Int, MainGraph[_, _]) => Boolean = (u : Int, graph : MainGraph[_, _]) => {
    val rigthU = graph.getVertex(u).getVertexOriginalId
    kcores.get(rigthU) match {
      /*
         Every clique of size k + 1 is a subgraph of the k-core because
         right before the algorithm removes the first node from the clique,
         that node has a degree of at least k

         All vertices in a clique of size k have core numbers of at least k−1
       */
      case Some(v_kcore) => v_kcore >= size
      case None => false
    }
  }



  def addFrozenData(pFrozenData: FrozenDataHolder): Unit = {
    if (pFrozenData.getSize <= 2) { //get rid of single edges
      return
    }
    synchronized {
      if (frozenMap == null) initFrozenMap()
      val id = counter
      counter += 1
      frozenMap.put(id, SynchronizedNodeBuilder.serialiseFrozenDataHolder(pFrozenData))
      val size = pFrozenData.getSize
      val ids = sizesMap.getOrElseUpdate(size, new mutable.HashSet[Int])
      ids.add(id)
      sizesMap.put(size, ids)
      availableSizes.add(size)
    }
  }

  def initFrozenMap(): Unit = {
    frozenMap = ChronicleMap
      .of(classOf[Integer], classOf[Array[Byte]])
      .name("frozen-map")
      .averageValue(makeDummyGraph()) //TODO
      .entries(10000000) //TODO
      .create
  }

  def pollFirstAvailable(size : Int, cliques : List[Set[Int]]): FrozenDataHolder = lock.synchronized{
      var availableSize = availableSizes.lastKey
      while (availableSizes.nonEmpty) {
        if (availableSize >= size) {
          sizesMap.get(availableSize) match {
            case Some(ids) =>
              val graphNew: Array[Byte] = frozenMap.get(ids.head)
              if (ids.size == 1) {
                sizesMap.remove(availableSize)
                availableSizes.remove(availableSize)
              } else {
                sizesMap.put(availableSize, ids.tail)
              }
              frozenMap.remove(ids.head)
              val frozenDataHolder = SynchronizedNodeBuilder.deserialiseFrozenDataHolder(graphNew)
              if (cliques.isEmpty || isHolderOk(cliques, frozenDataHolder)) {
                if (frozenDataHolder.getSize >= size) {
                  return frozenDataHolder
                } else {
                  //sizes of holder were changed, so we should to resave holder
                  addFrozenData(frozenDataHolder)
                }
              }
            case None =>
              sizesMap.remove(availableSize)
          }
          if (availableSizes.nonEmpty) {
            availableSize = availableSizes.lastKey
          }
        } else {
          return null
        }
      }
      null
  }

  def makeDummyGraph(): Array[Byte] = {
    val n = new SynchronizedDynamicNode(0)
    n.addOutBoundNodes(List(1, 2, 3, 5, 6, 7, 1, 2, 3, 5, 6, 7))

    val n1 = new SynchronizedDynamicNode(4)
    n1.addOutBoundNodes(List(5, 6, 7, 1, 2, 3, 5, 6, 7))

    val n2 = new SynchronizedDynamicNode(8)
    n2.addOutBoundNodes(List(9, 10, 11, 1, 2, 3, 5, 6, 7))

    val n3 = new SynchronizedDynamicNode(0)
    n3.addOutBoundNodes(List(1, 2, 3, 5, 6, 7, 1, 2, 3, 5, 6, 7))

    val n4 = new SynchronizedDynamicNode(4)
    n4.addOutBoundNodes(List(5, 6, 7, 1, 2, 3, 5, 6, 7))

    val n5 = new SynchronizedDynamicNode(8)
    n5.addOutBoundNodes(List(9, 10, 11, 1, 2, 3, 5, 6, 7))

    val nodes = List[SynchronizedDynamicNode](n, n1, n2, n3, n4, n5)

    val frozenDataHolder = new FrozenDataHolder()
    frozenDataHolder.freezePrefix = Seq(1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4)
    frozenDataHolder.freezeDag = nodes
//    frozenDataHolder.freezeDag = new SynchronizedDynamicGraphV2(nodes)
//    for (i <- nodes.indices) {
//      frozenDataHolder.freezeDag.getOrCreateNode(i)
//    }
    SynchronizedNodeBuilder.serialiseFrozenDataHolder(frozenDataHolder)
  }

  def isHolderOk(cliques : List[Set[Int]], frozenDataHolder: FrozenDataHolder) : Boolean = {
    for (clique <- cliques) {
      if (frozenDataHolder.isPrefixInClique(clique)) {
        return false
      }
      if (frozenDataHolder.freezePrefix.exists(clique.contains)) {
        frozenDataHolder.clearDag(clique)
      }
      if (frozenDataHolder.getSize <= 2) {
        //get rid of single edges
        return false
      }
    }
    true
  }

  def isEmpty: Boolean = {
    synchronized {
      availableSizes.isEmpty
    }
  }

  def close(): Unit = {
    frozenMap.close()
  }

}
