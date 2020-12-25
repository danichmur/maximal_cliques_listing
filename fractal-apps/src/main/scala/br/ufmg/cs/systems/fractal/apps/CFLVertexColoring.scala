package br.ufmg.cs.systems.fractal


import br.ufmg.cs.systems.fractal.GraphColoring.logWarning
import br.ufmg.cs.systems.fractal.apps.MaximalCliquesListing.logWarning
import br.ufmg.cs.systems.fractal.gmlib.clique.{GraphInner, KClistEnumerator}
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, Pregel, VertexId}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel
import org.graphframes.GraphFrame

import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Random
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


/**
 * A pregel implementation of a randomized graph coloring algorithm.
 * This algorithm finds a proper coloring in exponential time with high probability,
 * given that a coloring does exists.
 *
 * Reference:
 * Leith, D. J., and P. Clifford. "Convergence of Distributed Learning Algorithms
 * for Optimal Wireless Channel Allocation." IEEE Conference on Decision and Control, 2006.
 */
object CFLVertexColoring extends Logging {

  type Color = Long
  type EdgeWeight = Double

  type Palette = (Color, List[Double], Boolean, Random)

  private def sampleColor (dist : List[Double], rnd : Double) : Color = {
    dist.foldLeft((1, 0.0)) {
      case ((color, mass), weight) => {
        val m = mass + weight
        (if (m < rnd) color + 1 else color, m)
      }
    }._1
  }

  def apply[VD : ClassTag] (
                             graph : Graph[VD, _],
                             beta : Double,
                             maxNumColors : Long, // Colors are values between 1 and maxNumColors
                             isConnected : Boolean = false
                           ) : Graph[Color, _] = {
    val seed = Random.nextLong
    val space = Random.nextInt
    val initColorDist = (1L to maxNumColors).toList.map(_ => 1.0 / maxNumColors)
    val distGraph = graph.mapVertices((id, attr) => {
      val rng = new Random(seed + id * space)
      (rng.nextLong % maxNumColors + 1, initColorDist, true, rng)
    })

    def sendMessage (edge : EdgeTriplet[Palette, _]) : Iterator[(VertexId, Boolean)] = {
      if (edge.srcAttr._1 == edge.dstAttr._1)
        return Iterator((edge.srcId, true))
      if (edge.srcAttr._3)
        return Iterator((edge.srcId, false))
      Iterator.empty
    }
    def vprog (id : VertexId, attr : Palette, active : Boolean) : Palette = {
      val color = attr._1
      val dist = attr._2
      val rng = attr._4
      val new_dist = dist.foldLeft((1, List[Double]())) {
        case ((i, list), weight) => (i + 1,
          if (active) {
            list :+ (weight * (1 - beta) + (if (color == i) 0.0 else beta / (maxNumColors - 1)))
          } else {
            list :+ (if (color == i) 1.0 else 0.0)
          })
      }._2
      val new_color = if (active) sampleColor(new_dist, rng.nextDouble) else color
      (new_color, new_dist, active, rng)
    }
    val colorGraph = Pregel(distGraph, true)(vprog, sendMessage, _ || _).mapVertices((_, attr) => attr._1)
    if (isConnected) {
      colorGraph
    } else {
      graph.outerJoinVertices(colorGraph.vertices)((vid, _, opt) => opt.getOrElse(1))
    }
  }

  def findInvalidEdges[ED : ClassTag] (
                                        graph : Graph[Color, ED],
                                        maxNumColors : Long,
                                        maxNumMessages : Int = 1
                                      ) : Seq[String] = {
    graph.mapTriplets[String]((e : EdgeTriplet[Color, ED]) =>
      if (e.srcAttr == e.dstAttr)
        "Vertex " + e.srcId + " and Vertex " + e.dstId + " share the same color " + e.srcAttr
      else if (e.srcAttr > maxNumColors)
        "Vertex " + e.srcId + " has invalid color: " + e.srcAttr
      else if (e.dstAttr > maxNumColors)
        "Vertex " + e.dstId + " has invalid color: " + e.dstAttr
      else ""
    ).edges.filter(e => e.attr != "").map(e => e.attr).take(maxNumMessages)
  }

  def verify (graph : Graph[Color, _], maxNumColors : Long) : Boolean = {
    findInvalidEdges(graph, maxNumColors).size == 0
  }

  def line2edge(string: String): GraphInner.Edge1 = {
    val array = string.split(" ")
    new GraphInner.Edge1(array(0).toInt, array(1).toInt);
  }

  def toInt(x: Any): Int = x match {
    case i: Int => i
    case _ => 0
  }

  def countAndSetColors(path : String): Long = {
    val startTimeMillis = System.currentTimeMillis()
    val source = Source.fromFile(path)

    val graphInner = new GraphInner()
    try {
      for (line <- source.getLines) {
        graphInner.addEdge(line2edge(line))
      }
    } finally {
      source.close()
    }

    println(s"Reading: ${(System.currentTimeMillis() - startTimeMillis) / 1000} s")

    KClistEnumerator.countAndSetColors(graphInner)

    //val colors = KClistEnumerator.getColors

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

    durationSeconds
  }

  def setColors(sc: SparkContext): Unit = {

    val line = sc.textFile("/Users/danielmuraveyko/Desktop/colors.txt").collect()(0)
    val colors0 = line.split(", ")
    val colors = colors0.map(x => new Integer(x.toInt))

    KClistEnumerator.setColors(colors)
  }
}