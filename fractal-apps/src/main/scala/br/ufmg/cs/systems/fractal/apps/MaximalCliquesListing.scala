package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal.computation.{Computation, Refrigerator}
import br.ufmg.cs.systems.fractal.graph.Edge
import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.{EdgeFilterFunc, Logging}
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

    //https://dl.acm.org/citation.cfm?id=3186125
    //Fractoid with the initial state for cliques
    val initialFractoid = fractalGraph.vfractoid.expand(1)

    val cliquesRes = initialFractoid.
      //set("efilter", epredCallback(readyCliques)).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

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

    val graphPath = "/Users/danielmuraveyko/Desktop/for_kcore_18"

    val sc = new SparkContext(conf)
    sc.setLogLevel(logLevel)

    val kcore_list = Kcore.countKcore(sc, graphPath)
    val kcore = kcore_list.map(_._2).distinct
    val kcore_map = kcore_list.toMap

    val fc = new FractalContext(sc)

    val graphClass = "br.ufmg.cs.systems.fractal.graph.EdgeListGraph"
    val fractalGraph = fc.textFile(graphPath, graphClass = graphClass)
    val commStrategy = "scratch"
    val numPartitions = 1
    var explorationSteps = kcore.head

    var cliques : List[Set[Int]] = List()
    var cliquesIdx : List[Set[Int]] = List()

    val addCliques = (steps : Int) => {
      val app = CliquesList(fractalGraph, commStrategy, numPartitions, steps, cliquesIdx)
      val (subgraphs, original_cliques) = app.findCliques()
      cliques = cliques ++ original_cliques
      cliquesIdx = cliquesIdx ++ subgraphs
    }

    val N = 4 //cliques count
    val time = System.currentTimeMillis()
    fractalGraph.set("kcores", kcore_map)

//    while (cliques.size < N && explorationSteps >= 2) {
//      if (!Refrigerator.isEmpty) {
//        Refrigerator.freeze = true
//        val foundFistFrozenData = Refrigerator.pollFirstAvailable(explorationSteps, cliquesIdx)
//        if (foundFistFrozenData != null) {
//          fractalGraph.set("cliquesize", explorationSteps + 1)
//          Refrigerator.current = foundFistFrozenData
//          addCliques(explorationSteps - foundFistFrozenData.freezePrefix.size)
//        } else {
//          explorationSteps -= 1
//        }
//      } else if (Refrigerator.freeze) {
//        //The frozen list is empty, we are done here
//        explorationSteps = 0
//      } else {
//        fractalGraph.set("cliquesize", 71)
//        addCliques(71)
//        explorationSteps -= 1
//      }

      logWarning(s"explorationSteps: ${explorationSteps + 1} done")

    fractalGraph.set("cliquesize", 71)
    addCliques(71)

//      if (cliques.size > N) {
//        cliques = cliques.slice(0, N)
//      }
//    }

    logWarning("Result: " + cliques.toString)
    logWarning(s"Time: ${(System.currentTimeMillis() - time) / 1000.0}s\n")

    //Refrigerator.close()
    fc.stop()
    sc.stop()
  }
}