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

  var foundedCliques : (List[Set[Int]], List[Set[Int]]) = (List(), List())

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
    val cliques = cliquesRes.collectSubgraphs();
    foundedCliques = (cliques, cliquesRes.collectSubgraphsOriginal(cliques))
  }

  def findCliques(): (List[Set[Int]], List[Set[Int]]) = {
    execute
    foundedCliques
  }
}

object MaximalCliquesListing extends Logging {
 //525 v, 22415 e - 8 min

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("MaximalCliquesListing")
    conf.set("spark.executor.memory", "16g")
    conf.set("spark.driver.memory","16g")
    val graphPath = "/Users/danielmuraveyko/Desktop/for_kcore_0"

    val sc = new SparkContext(conf)
    val kcore = Kcore.countKcore(sc, graphPath).map(_._2).distinct

    val fc = new FractalContext(sc)

    val graphClass = "br.ufmg.cs.systems.fractal.graph.EdgeListGraph"
    val fractalGraph = fc.textFile("/Users/danielmuraveyko/Desktop/for_kcore_0", graphClass = graphClass)
    val commStrategy = "scratch"
    val numPartitions = 1
    var explorationSteps = kcore.head

    val outPath = "/Users/danielmuraveyko/Desktop/test"
    var cliques : List[Set[Int]] = List()
    var cliquesIdx : List[Set[Int]] = List()

    val N = 3 //cliques count
    while (cliques.size < N && explorationSteps >= 2) {

      fractalGraph.set("cliquesize", explorationSteps)

      val app = CliquesList(fractalGraph, commStrategy, numPartitions, explorationSteps, cliquesIdx)
      explorationSteps -= 1
      val (subgraphs, original_cliques) = app.findCliques()//.map(x => x.flatMap(toInt))
      logInfo(s"explorationSteps: ${explorationSteps} done")

      cliques = cliques ++ original_cliques
      cliquesIdx = cliquesIdx ++ subgraphs

      if (cliques.size > N) {
        cliques = cliques.slice(0, N)
      }
    }

    logInfo(cliques.toString)

    fc.stop()
    sc.stop()
  }
}