package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal.computation.{Computation, Refrigerator}
import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.gmlib.clique.KClistEnumerator
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.{EdgeFilterFunc, Logging}
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._

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

//    def epredCallback(cliques : List[Set[Int]]) = {
//      new EdgeFilterFunc[EdgeInducedSubgraph] {
//        override def test(e: Edge[EdgeInducedSubgraph]): Boolean = {
//          !cliques.exists(c => c.contains(e.getSourceId) && c.contains(e.getDestinationId))
//        }
//      }
//    }

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

    testF.setNew(explorationSteps, kcore_map)

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
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

   // val graphPath = "/Users/danielmuraveyko/Desktop/els2/for_kcore_600"
    //val graphPath = "/Users/danielmuraveyko/Desktop/els2/for_kcore_900"
    val graphPath = "/Users/danielmuraveyko/Desktop/els2/for_kcore_1200"
    //val graphPath = "/Users/danielmuraveyko/Desktop/els/for_kcore_0"
  //  val graphPath = "/Users/danielmuraveyko/Desktop/els2/for_kcore_260"
  //  val graphPath = "/Users/danielmuraveyko/Desktop/els/for_kcore_4"
    //val graphPath = "/Users/danielmuraveyko/Desktop/els2/for_kcore_300"

    val sc = new SparkContext(conf)
    sc.setLogLevel(logLevel)


    //--------------------------------------------------

//
//    val users: RDD[(VertexId, Int)] = sc.parallelize(Seq((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)))
//    // Create an RDD for edges
//    val relationships: RDD[Edge[Int]] = sc.parallelize(Seq(
//      Edge(1, 2, 0), Edge(1, 3, 0), Edge(2, 3, 0), Edge(1, 4, 0), Edge(3, 4, 0), Edge(2, 4, 0),
//      Edge(4, 5, 0), Edge(5, 6, 0), Edge(4, 6, 0)
//    ))
//
//    val graph = Graph(users, relationships)
//
//    val t1 = graph.triplets.filter(_.srcId == 1).map(_.dstId)
//    val t2 = graph.triplets.filter(_.srcId == 2).map(_.dstId)
//    val t3 = graph.triplets.filter(_.srcId == 3).map(_.dstId).collect
//    val t4 = graph.triplets.filter(_.srcId == 4).map(_.dstId).collect
//    val t5 = graph.triplets.filter(_.srcId == 5).map(_.dstId).collect
//    val t6 = graph.triplets.filter(_.srcId == 6).map(_.dstId).collect
//
//    t1.intersection(t2).foreach(println)

//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//
//    val v = sqlContext.createDataFrame(List(Tuple1(1), Tuple1(2), Tuple1(3), Tuple1(4), Tuple1(5), Tuple1(6))).toDF("id")
//    // Edge DataFrame
//    val e = sqlContext.createDataFrame(List((1, 2), (1, 3), (2, 3), (1, 4), (3, 4), (2, 4), (4, 5), (5, 6), (4, 6))).toDF("src", "dst")
//
//    val g = GraphFrame(v, e)
//    g.degrees.sort("id").show(20, false)
//
//    g.triplets.filter("src.id == 1")


    //    val neo = Neo4j(sc)
//    val graphQuery = "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN id(n) as source, id(m) as target, type(r) as value SKIP $_skip LIMIT $_limit"
//    val graph: Graph[Long, String] = neo.rels(graphQuery).partitions(7).batch(200).loadGraph
//
//    logWarning(graph.vertices.count.toString)
//    logWarning(graph.edges.count.toString)
//
//    sc.stop()
//    return

    //--------------------------------------------------

    val fc = new FractalContext(sc)

    logWarning(sc.uiWebUrl.getOrElse(""))

    val graphClass = "br.ufmg.cs.systems.fractal.graph.EdgeListGraph"
    val fractalGraph = fc.textFile(graphPath, graphClass = graphClass)
    val commStrategy = "scratch"
    val numPartitions = 1
    //TODO: if a graph can be colored with k colors, then the maximum clique in this graph must be smaller or equal to k
    //var explorationSteps = kcore.head


    var cliques : List[Set[Int]] = List()
    var cliquesIdx : List[Set[Int]] = List()

    val addCliques = (steps : Int) => {
      val app = CliquesList(fractalGraph, commStrategy, numPartitions, steps, cliquesIdx, Map.empty)
      val (subgraphs, original_cliques) = app.findCliques()
      cliques = cliques ++ original_cliques
      cliquesIdx = cliquesIdx ++ subgraphs
    }

    val time = System.currentTimeMillis()
    Refrigerator.start = time

    // val s = 2400
    //val s = 3600
    val s = 4800
    //val s = 3
   // val s = 1040
  // val s = 16
   // val s = 1200

    addCliques(s)

    logWarning("extends: " + KClistEnumerator.count.toString)
    logWarning(s"Time: ${(System.currentTimeMillis() - time) / 1000.0}s\n")

    for (r <- Refrigerator.result) {
      //TODO vertex original ids
      r.toArray()
      println(r.size) //toArray.sorted.deep.mkString(", "))
    }

    fc.stop()
    sc.stop()
  }
}