package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.graph.{Edge, Vertex}
import br.ufmg.cs.systems.fractal.{CliquesOptApp, _}
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, ResultSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.{EdgeFilterFunc, Logging, VertexFilterFunc}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, GraphLoader, PartitionStrategy, VertexId}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

case class CliquesList(
                        fractalGraph: FractalGraph,
                        commStrategy: String,
                        numPartitions: Int,
                        explorationSteps: Int,
                        readyCliques: List[Set[Int]]
) extends FractalSparkApp {
  var foundedCliques : List[Set[_]] = List()

  def execute: Unit = {

    def epredCallback(cliques : List[Set[Int]]) = {
      new EdgeFilterFunc[EdgeInducedSubgraph] {
        override def test(e: Edge[EdgeInducedSubgraph]): Boolean = {
          !cliques.exists(c => c.contains(e.getSourceId) && c.contains(e.getDestinationId))
        }
      }
    }

    val vfilter = (v : VertexInducedSubgraph, c : Computation[VertexInducedSubgraph]) => {
      true
    }

    val cliquesRes = fractalGraph.cliquesKClist(explorationSteps + 1).
      //filter(vfilter).
      set("efilter", epredCallback(readyCliques)).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (_, elapsed) = FractalSparkRunner.time {
      cliquesRes.compute()
    }

    logInfo (s"CliquesList comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${cliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
    )
    foundedCliques = cliquesRes.collectSubgraphs()
  }

  def findCliques(): List[Set[_]] = {
    execute
    foundedCliques
  }
}

object MaximalCliquesListing extends Logging {

  def toInt(x: Any): Option[Int] = x match {
    case i: Int => Some(i)
    case _ => None
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("MaximalCliquesListing")

    val sc = new SparkContext(conf)
    val kcore = Kcore.countKcore(sc, "/Users/danielmuraveyko/Desktop/for_kcore").map(_._2).distinct

    val fc = new FractalContext(sc)
    val graphPath = "/Users/danielmuraveyko/Desktop/el_8.csv"

    val graphClass = "br.ufmg.cs.systems.fractal.graph.BasicMainGraph"
    val fractalGraph = fc.textFile(graphPath, graphClass = graphClass)
    val commStrategy = "scratch"
    val numPartitions = 1
    var explorationSteps = kcore.head - 1

    val outPath = "/Users/danielmuraveyko/Desktop/test"
    var cliques : List[Set[Int]] = List()

    val N = 15 //cliques count
    while (cliques.size < N && explorationSteps > 2) {

      fractalGraph.set("cliquesize", explorationSteps)

      val app = CliquesList(fractalGraph, commStrategy, numPartitions, explorationSteps, cliques)
      explorationSteps -= 1
      val subgraphs = app.findCliques().map(x => x.flatMap(toInt))
      logInfo(s"explorationSteps: ${explorationSteps} done")
      cliques = cliques ++ subgraphs

      if (cliques.size > N) {
        cliques = cliques.slice(0, N)
      }
    }

    logInfo(cliques.toString)

    fc.stop()
    sc.stop()
  }
}