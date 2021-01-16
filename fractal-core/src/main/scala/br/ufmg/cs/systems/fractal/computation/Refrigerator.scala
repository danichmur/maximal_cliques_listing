package br.ufmg.cs.systems.fractal.computation

import java.util
import java.util.HashSet

import br.ufmg.cs.systems.fractal.gmlib.clique.FrozenDataHolderOld
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.subgraph.Subgraph
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
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

  var result : List[IntArrayList] = List.empty
  var neigh_sizes : util.List[Integer] = _

  var idx : Int = 0

  var size : Int = 0

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

  def inc(): Unit = {
    idx += 1
  }


}
