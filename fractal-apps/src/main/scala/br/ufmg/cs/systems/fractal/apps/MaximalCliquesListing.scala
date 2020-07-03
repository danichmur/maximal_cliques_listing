package br.ufmg.cs.systems.fractal.apps

import java.util

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.clique.{FrozenDataHolder, GlobalFreezeHolder}
import br.ufmg.cs.systems.fractal.graph.Edge
import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.{EdgeFilterFunc, Logging}
import org.apache.spark.{SparkConf, SparkContext}

import collection.JavaConverters._

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

    //https://dl.acm.org/citation.cfm?id=3186125
    //Fractoid with the initial state for cliques
    val initialFractoid = fractalGraph.vfractoid.expand(1)

    val cliquesRes = initialFractoid.
      set("efilter", epredCallback(readyCliques)).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (_, elapsed) = FractalSparkRunner.time {
      cliquesRes.compute()
    }
//
//    logInfo (s"CliquesList comm=${commStrategy}" +
//      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
//      s" graph=${fractalGraph} " +
//      s" numValidSubgraphs=${cliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
//    )
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
    val logLevel = "WARN"
    conf.set("spark.executor.memory", "16g")
    conf.set("spark.driver.memory","16g")
    conf.set("fractal.log.level", logLevel)

    val graphPath = "/Users/danielmuraveyko/Desktop/els/for_kcore_15"

    val sc = new SparkContext(conf)
    sc.setLogLevel(logLevel)

    val kcore = Kcore.countKcore(sc, graphPath).map(_._2).distinct

    val fc = new FractalContext(sc)

    val graphClass = "br.ufmg.cs.systems.fractal.graph.EdgeListGraph"
    val fractalGraph = fc.textFile(graphPath, graphClass = graphClass)
    val commStrategy = "scratch"
    val numPartitions = 1
    var explorationSteps = kcore.head

    var cliques : List[Set[Int]] = List()
    var cliquesIdx : List[Set[Int]] = List()
    val cliquesIdxJava : java.util.List[java.util.Set[Integer]] = new util.ArrayList[java.util.Set[Integer]]()

    val addCliques = (steps : Int) => {
      val app = CliquesList(fractalGraph, commStrategy, numPartitions, steps, cliquesIdx)
      val (subgraphs, original_cliques) = app.findCliques()
      cliques = cliques ++ original_cliques
      cliquesIdx = cliquesIdx ++ subgraphs
      cliquesIdxJava.addAll(subgraphs.map(x => x.map(i => new Integer(i)).asJava).asJava)
    }

    val N = 10 //cliques count

    while (cliques.size < N && explorationSteps >= 2) {
      if (GlobalFreezeHolder.isFrozenAvailable) {
        GlobalFreezeHolder.freeze = true
        val foundFistFrozenData = GlobalFreezeHolder.pollFirstAvailable(explorationSteps, cliquesIdxJava)
        if (foundFistFrozenData != null) {
          fractalGraph.set("cliquesize", explorationSteps + 1)
          GlobalFreezeHolder.current = foundFistFrozenData
          addCliques(explorationSteps - foundFistFrozenData.freezePrefix.size() )
        } else {
          explorationSteps -= 1
        }
      } else if (GlobalFreezeHolder.freeze) {
        //The frozen list is empty, we are done here
        explorationSteps = 0
      } else {
        fractalGraph.set("cliquesize", explorationSteps + 1)
        addCliques(explorationSteps)
        explorationSteps -= 1
      }

      logWarning(s"explorationSteps: ${explorationSteps + 1} done")

      if (cliques.size > N) {
        cliques = cliques.slice(0, N)
      }
    }
    GlobalFreezeHolder.deleteFrozenDir()

    logWarning("Result: " + cliques.toString)

    fc.stop()
    sc.stop()
  }
}