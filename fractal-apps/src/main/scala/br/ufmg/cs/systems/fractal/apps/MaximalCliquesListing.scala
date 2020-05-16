package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal.graph.{Edge, Vertex}
import br.ufmg.cs.systems.fractal.{CliquesOptApp, _}
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, ResultSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.{EdgeFilterFunc, Logging, VertexFilterFunc}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, GraphLoader, PartitionStrategy, VertexId}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.IntMap

case class CliquesList(val fractalGraph: FractalGraph,
                  commStrategy: String,
                  numPartitions: Int,
                  explorationSteps: Int
) extends FractalSparkApp {
  var foundedCliques : List[ResultSubgraph[_]] = List()
  var s : Set[Int] = Set()

  def execute: Unit = {

//    val vpred = new VertexFilterFunc[VertexInducedSubgraph] {
//      override def test(v: Vertex[VertexInducedSubgraph]): Boolean = {
//        //  v.getVertexLabel() < 170
//        //true;
//        !s.contains(v.getVertexId)
//      }
//    }

    val epred = new EdgeFilterFunc[EdgeInducedSubgraph] {
      override def test(e: Edge[EdgeInducedSubgraph]): Boolean = {
        !(s.contains(e.getSourceId) || s.contains(e.getDestinationId))
      }
    }


    val cliquesRes = fractalGraph.cliquesKClist(explorationSteps + 1).
      //set("efilter", epred).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (_, elapsed) = FractalSparkRunner.time {
      cliquesRes.compute()
    }

    logInfo (s"CliquesOptApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${cliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
    )
    foundedCliques = cliquesRes.collectSubgraphs()
  }

  def findCliques(): List[ResultSubgraph[_]] = {
    execute
    foundedCliques
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
    fractalGraph.set("cliquesize", explorationSteps)

    val outPath = "/Users/danielmuraveyko/Desktop/test"

    val app = CliquesList(fractalGraph, commStrategy, numPartitions, explorationSteps)

    val subgraphs = app.findCliques()
    logInfo(subgraphs.toString)

    fc.stop()
    sc.stop()
  }
}