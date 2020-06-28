package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal.computation.Computation
import br.ufmg.cs.systems.fractal.gmlib.clique.{FrozenDataHolder, GlobalFreezeHolder}
import br.ufmg.cs.systems.fractal.graph.{Edge, Vertex}
import br.ufmg.cs.systems.fractal.{CliquesOptApp, _}
import br.ufmg.cs.systems.fractal.pattern.Pattern
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, ResultSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool
import br.ufmg.cs.systems.fractal.util.{EdgeFilterFunc, Logging, VertexFilterFunc}
import com.koloboke.collect.map.IntObjMap
import com.koloboke.collect.map.hash.HashIntObjMaps
import org.apache.hadoop.io.LongWritable
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, GraphLoader, PartitionStrategy, VertexId}
import org.apache.spark.storage.StorageLevel
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
    conf.set("spark.executor.memory", "16g")
    conf.set("spark.driver.memory","16g")
    val graphPath = "/Users/danielmuraveyko/Desktop/els/for_kcore_0"

    val sc = new SparkContext(conf)
    val kcore = Kcore.countKcore(sc, graphPath).map(_._2).distinct

    val fc = new FractalContext(sc)

    val graphClass = "br.ufmg.cs.systems.fractal.graph.EdgeListGraph"
    val fractalGraph = fc.textFile(graphPath, graphClass = graphClass)
    val commStrategy = "scratch"
    val numPartitions = 1
    var explorationSteps = kcore.head

    val outPath = "/Users/danielmuraveyko/Desktop/test"
    var cliques : List[Set[Int]] = List()
    var cliquesIdx : List[Set[Int]] = List()

    fractalGraph.set("cliquesize", explorationSteps)
    var app = CliquesList(fractalGraph, commStrategy, numPartitions, explorationSteps, cliquesIdx)
    val (subgraphs, original_cliques) = app.findCliques()
    logInfo(s"explorationSteps: ${explorationSteps} done")
    cliques = cliques ++ original_cliques
    cliquesIdx = cliquesIdx ++ subgraphs

    GlobalFreezeHolder.freeze = true

    val cliquesIdxJava = cliquesIdx.map(x => x.map(i => new Integer(i)).asJava).asJava
    GlobalFreezeHolder.cleanFrozenList(cliquesIdxJava)

    val list = GlobalFreezeHolder.getFrozenList
    val copy = new java.util.ArrayList[FrozenDataHolder]()

    copy.addAll(list)

    val iterator = copy.iterator()

    explorationSteps -= 1
    while (iterator.hasNext) {
      val elem = iterator.next()
      if (elem.freezeDag.size + elem.freezePrefix.size > explorationSteps) {
        GlobalFreezeHolder.current = elem
        fractalGraph.set("cliquesize", explorationSteps)
        app = CliquesList(fractalGraph, commStrategy, numPartitions, explorationSteps - elem.freezePrefix.size(), cliquesIdx)
        val (subgraphs2, original_cliques2) = app.findCliques()
        logInfo(s"explorationSteps: ${1} done")
        cliques = cliques ++ original_cliques2
        cliquesIdx = cliquesIdx ++ subgraphs2
      }

    }



    val N = 1 //cliques count
//    while (cliques.size < N && explorationSteps >= 2) {
//
//      fractalGraph.set("cliquesize", explorationSteps +1)
//
//      val app = CliquesList(fractalGraph, commStrategy, numPartitions, explorationSteps, cliquesIdx)
//      explorationSteps -= 1
//      val (subgraphs, original_cliques) = app.findCliques()
//      logInfo(s"explorationSteps: ${explorationSteps} done")
//
//      cliques = cliques ++ original_cliques
//      cliquesIdx = cliquesIdx ++ subgraphs
//
//      if (cliques.size > N) {
//        cliques = cliques.slice(0, N)
//      }
//    }

    logInfo(cliques.toString)

    fc.stop()
    sc.stop()
  }
}