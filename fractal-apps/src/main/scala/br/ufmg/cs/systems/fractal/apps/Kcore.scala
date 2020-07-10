package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal.apps.MaximalCliquesListing.logInfo
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, GraphLoader, PartitionStrategy, VertexId}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.IntMap

object Kcore extends Logging {

  val initialMsg="-10"
  def mergeMsg(msg1: String, msg2:String): String = msg1+":"+msg2

  def vprog(vertexId: VertexId, value: (Int, Int, IntMap[Int], Int), message: String): (Int, Int, IntMap[Int], Int) = {
    if (message == initialMsg){
      (value._1,value._2,value._3,value._4)
    } else {
      val msg=message.split(":")
      val elems=msg //newWeights.values
      val counts: Array[Int] = new Array[Int](value._1 + 1)
      for (m <-elems){
        val im=m.toInt
        if(im<=value._1) {
          counts(im)=counts(im)+1
        } else {
          counts(value._1)=counts(value._1)+1
        }
      }
      var curWeight =  0 //value._4-newWeights.size
      for(i<-value._1 to 1 by -1) {
        curWeight=curWeight+counts(i)
        if(i<=curWeight) {
          return (i, value._1,value._3,value._4)
        }
      }
      (0, value._1,value._3,value._4)
    }
  }

  def sendMsg(triplet: EdgeTriplet[(Int, Int,IntMap[Int],Int), Int]): Iterator[(VertexId, String)] = {
    val sourceVertex = triplet.srcAttr
    val destVertex=triplet.dstAttr
    Iterator((triplet.dstId,sourceVertex._1.toString),(triplet.srcId,destVertex._1.toString))
  }

  def countKcore(sc: SparkContext, path: String): List[(VertexId, Int)] = {
    // TODO: figure out
    val maxIter = 10

    val startTimeMillis = System.currentTimeMillis()

    // TODO: edgeListFile input format is completely different from Fractal input format :(
    val ygraph=GraphLoader.edgeListFile(
      sc,
      path,
      canonicalOrientation = true,
      //TODO: figure out
      -1,
      StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_AND_DISK
    ).partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((e1, _) => e1)

    val deg=ygraph.degrees

    val mgraph=ygraph.outerJoinVertices(deg)((_, _, newattr) =>newattr.getOrElse(0)).mapVertices((_, attr) =>(attr,-1,IntMap[Int](),attr))
    ygraph.unpersist()

    val minGraph = mgraph.pregel(initialMsg, maxIter, EdgeDirection.Either)(vprog,sendMsg,mergeMsg)

    val kcores = minGraph.vertices.zipWithIndex.sortBy(e => -e._1._2._1).map(e => (e._1._1, e._1._2._1)).collect.toList
    //.sortBy(l => l._1._2._1).foreach{case (e) => println(e._1._1 + "," + e._1._2._1)}

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    logInfo(s"Total Execution Kcore Time : ${durationSeconds.toString()}s")
    kcores
  }
}
