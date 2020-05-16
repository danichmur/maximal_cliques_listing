package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal.{CliquesOptApp, _}
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.hadoop.io.LongWritable
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, GraphLoader, PartitionStrategy, VertexId}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.IntMap

case class CliquesList(val fractalGraph: FractalGraph,
                  commStrategy: String,
                  numPartitions: Int,
                  explorationSteps: Int,
                  outPath: String
) extends FractalSparkApp {
  def execute: Unit = {

    //TODO filters

    //      var s : Set[Int] = Set(vertices)
    //
    //      val vpred = new VertexFilterFunc[VertexInducedSubgraph] {
    //        override def test(v: Vertex[VertexInducedSubgraph]): Boolean = {
    //          //  v.getVertexLabel() < 170
    //          //true;
    //          !s.contains(v.getVertexId)
    //        }
    //      }
    //
    //      val epred = new EdgeFilterFunc[EdgeInducedSubgraph] {
    //        override def test(e: Edge[EdgeInducedSubgraph]): Boolean = {
    //          !(s.contains(e.getSourceId) || s.contains(e.getDestinationId))
    //        }
    //      }


    val cliquesRes = fractalGraph.cliquesKClist(explorationSteps + 1).
      //set("efilter", epred).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      cliquesRes.compute()
    }

    logInfo (s"CliquesOptApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${cliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
    )

    cliquesRes.saveSubgraphsAsTextFile(outPath)
  }
}


object MaximalCliquesListing extends Logging {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("MaximalCliquesListing")

    val sc = new SparkContext(conf)
    val kcore = Kcore.countKcore(sc, "/Users/danielmuraveyko/Desktop/for_kcore");

    val fc = new FractalContext(sc)
    val graphPath = "/Users/danielmuraveyko/Desktop/el_3.csv"

    val graphClass = "br.ufmg.cs.systems.fractal.graph.BasicMainGraph"
    val fractalGraph = fc.textFile(graphPath, graphClass = graphClass)
    val commStrategy = "scratch"
    val numPartitions = 1
    val explorationSteps = kcore.head._2 - 1 //k - 1 = clique size
    val outPath = "/Users/danielmuraveyko/Desktop/test"

    val app = CliquesList(fractalGraph, commStrategy, numPartitions, explorationSteps, outPath)

    app.execute

    fc.stop()
    sc.stop()
  }
}