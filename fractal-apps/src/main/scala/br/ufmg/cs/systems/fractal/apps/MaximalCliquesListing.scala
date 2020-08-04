package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal.computation.{Computation, Refrigerator}
import br.ufmg.cs.systems.fractal.graph.Edge
import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.gmlib.clique.KClistEnumerator
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.{EdgeFilterFunc, Logging}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

case class CliquesList(
                        fractalGraph: FractalGraph,
                        commStrategy: String,
                        numPartitions: Int,
                        explorationSteps: Int,
                        readyCliques: List[Set[Int]],
                        kcore_map: Map[Int, Int]
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

    //https://dl.acm.org/citation.cfm?id=3186125
    //Fractoid with the initial state for cliques
    val initialFractoid = fractalGraph.vfractoid.expand(1)

    val testF = initialFractoid.
      //set("efilter", epredCallback(readyCliques)).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions)

    testF.setNew(explorationSteps,kcore_map)

      val cliquesRes =
        testF.explore(explorationSteps)

    val (_, elapsed) = FractalSparkRunner.time {cliquesRes.compute()}

    logInfo (s"CliquesList comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps} elapsed=${elapsed}"
    )
    val cliques = cliquesRes.collectSubgraphs()
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
    val logLevel = "WARN"
    conf.set("spark.executor.memory", "16g")
    conf.set("spark.driver.memory","16g")
    conf.set("fractal.log.level", logLevel)

    val graphPath = "/Users/danielmuraveyko/Desktop/els/for_kcore_18"

    val sc = new SparkContext(conf)
    sc.setLogLevel(logLevel)

    val kcore_list = Kcore.countKcore(sc, graphPath)
    val kcore = kcore_list.map(_._2).distinct
    val kcore_map = Kcore.countKcore(sc, graphPath).map(verexId => (verexId._1.toInt, verexId._2)).toMap

    val fc = new FractalContext(sc)

    val graphClass = "br.ufmg.cs.systems.fractal.graph.EdgeListGraph"
    val fractalGraph = fc.textFile(graphPath, graphClass = graphClass)
    val commStrategy = "scratch"
    val numPartitions = 1
    var explorationSteps = kcore.head

    var cliques : List[Set[Int]] = List()
    var cliquesIdx : List[Set[Int]] = List()

    val addCliques = (steps : Int) => {
      val app = CliquesList(fractalGraph, commStrategy, numPartitions, steps, cliquesIdx, kcore_map)
      val (subgraphs, original_cliques) = app.findCliques()
      cliques = cliques ++ original_cliques
      cliquesIdx = cliquesIdx ++ subgraphs
    }

    val time = System.currentTimeMillis()

    val s = 71
    addCliques(s)

    logWarning("Result: " + cliques.toString)
    logWarning(KClistEnumerator.count.toString)
    logWarning(s"Time: ${(System.currentTimeMillis() - time) / 1000.0}s\n")

    fc.stop()
    sc.stop()
  }
}