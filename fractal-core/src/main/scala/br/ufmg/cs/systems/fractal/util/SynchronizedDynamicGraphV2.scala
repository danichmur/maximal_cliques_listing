package br.ufmg.cs.systems.fractal.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import com.twitter.cassovary.graph.{DynamicDirectedGraphHashMap, SynchronizedDynamicGraph}
import com.twitter.cassovary.graph.StoredGraphDir.OnlyOut
import com.twitter.cassovary.graph.node.{DynamicNode, SynchronizedDynamicNode}
import br.ufmg.cs.systems.fractal.computation.FrozenDataHolder
import com.twitter.cassovary.collections.CSeq

class SynchronizedDynamicGraphV2(nodes: List[SynchronizedDynamicNode]) extends DynamicDirectedGraphHashMap(OnlyOut) {
  override def nodeFactory(id: Int): SynchronizedDynamicNode = nodes(id)
}

object SynchronizedNodeBuilder {

  def arrayList2Seq(arr: IntArrayList): Seq[Int] = {
    arr.getBackingArray.toSeq.take(arr.getSize)
  }

  def seq2ArrayList(s: CSeq[Int]): IntArrayList = {
    new IntArrayList(s.toSeq.toArray)
  }

  def build(id : Int, nodeIds : IntArrayList): SynchronizedDynamicNode = {
    val node  = new SynchronizedDynamicNode(id)
    node.addOutBoundNodes(nodeIds.getBackingArray.take(nodeIds.getSize))
    node
  }

  def serialiseFrozenDataHolder(value: FrozenDataHolder) : Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value.freezePrefix)
    oos.writeObject(value.freezeDag.size)
    for (x <- value.freezeDag) {
      serialiseNode(x, oos)
    }
    oos.close()
    stream.toByteArray
  }

  def deserialiseFrozenDataHolder(bytes: Array[Byte]): FrozenDataHolder = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val prefixRaw : scala.Any = ois.readObject
    val sizeRaw : scala.Any = ois.readObject

    val frozenDataHolder = new FrozenDataHolder()

    sizeRaw match {
      case size : Int => prefixRaw match {
        case arr : Seq[Int] =>
          frozenDataHolder.freezePrefix = arr
          var nodes = List[SynchronizedDynamicNode]()

          for (_ <- 0 until size) {
            deserialiseNode(ois) match {
              case Some(node) => nodes = node :: nodes
            }
          }

          frozenDataHolder.freezeDag = nodes //new SynchronizedDynamicGraphV2(nodes)

//          for (i <- nodes.indices) {
//            frozenDataHolder.freezeDag.getOrCreateNode(i)
//          }
      }
    }
    ois.close()
    frozenDataHolder
  }

  def serialiseNode(value: DynamicNode, oos : ObjectOutputStream): Unit = {
    oos.writeObject(value.id)
    val outbounds = Array.fill(value.outboundNodes.length){0}
    var i = 0
    value.outboundNodes.foreach(x => {
      outbounds(i) = x
      i += 1
    })
    oos.writeObject(outbounds.length)
    oos.writeObject(outbounds)
  }

  def deserialiseNode(ois: ObjectInputStream): Option[SynchronizedDynamicNode] = {
    val idRaw : scala.Any = ois.readObject
    val sizeRaw : scala.Any = ois.readObject
    val arrRaw : scala.Any = ois.readObject
    val result : Option[SynchronizedDynamicNode] = idRaw match {
      case id : Int => sizeRaw match {
        case size : Int => arrRaw match {
          case arr: Array[Int] =>
            val node = new SynchronizedDynamicNode(id)
            node.addOutBoundNodes(arr.take(size))
            Some(node)
          case None => None
        }
        case None => None
      }
      case None => None
    }
    result
  }

}