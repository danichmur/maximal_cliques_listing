package br.ufmg.cs.systems.fractal.computation


import br.ufmg.cs.systems.fractal.util.{SynchronizedDynamicGraphV2, SynchronizedNodeBuilder}
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import com.koloboke.collect.map.hash.HashIntObjMaps
import com.koloboke.collect.map.{IntObjCursor, IntObjMap}
import com.twitter.cassovary.graph.node.SynchronizedDynamicNode

class FrozenDataHolder(_freezeDag: IntObjMap[IntArrayList], _freezePrefix: IntArrayList) {

  var freezeDag: List[SynchronizedDynamicNode] = _
  var freezePrefix: Seq[Int] = _
  init()

  def init() {
    freezePrefix = SynchronizedNodeBuilder.arrayList2Seq(_freezePrefix)
    val cur: IntObjCursor[IntArrayList] = _freezeDag.cursor()

    this.freezeDag = List[SynchronizedDynamicNode]()

    while (cur.moveNext) {
      freezeDag = SynchronizedNodeBuilder.build(cur.key, cur.value) :: freezeDag
    }

//    this.freezeDag = new SynchronizedDynamicGraphV2(nodes)
//
//    for (i <- nodes.indices) {
//      freezeDag.getOrCreateNode(i)
//    }
  }

  def this() {
    this(HashIntObjMaps.newMutableMap[IntArrayList], new IntArrayList)
  }


  def getSize: Int = freezeDag.size + freezePrefix.size

  def isPrefixInClique(clique : Set[Int]) : Boolean = {
    var i = 0
    while (i < clique.size - 1) {
      var j = i + 1
      while (j < clique.size) {
        //todo
        if (freezePrefix.contains(clique(i)) && freezePrefix.contains(clique(j))) {
          return true
        }
        j += 1
      }
      i += 1
    }
    false
  }

  def clearDag(clique : Set[Int]) : Unit = {
    freezeDag = freezeDag.filter(x => !clique.contains(x.id))
  }
}
