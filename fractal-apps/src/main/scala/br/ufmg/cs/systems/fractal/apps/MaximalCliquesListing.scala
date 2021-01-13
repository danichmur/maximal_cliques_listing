package br.ufmg.cs.systems.fractal.apps

import br.ufmg.cs.systems.fractal.CFLVertexColoring.logWarning
import br.ufmg.cs.systems.fractal.computation.Refrigerator
import br.ufmg.cs.systems.fractal._
import br.ufmg.cs.systems.fractal.gmlib.clique.KClistEnumerator
import br.ufmg.cs.systems.fractal.util.Logging
import org.apache.spark.{SparkConf, SparkContext}

case class CliquesList(
                        fractalGraph: FractalGraph,
                        explorationSteps: Int,
                        readyCliques: List[Set[Int]],
                        dataPath : String,
                        N : Int,
                        inc : Boolean
                      ) extends FractalSparkApp {

  var foundedCliques : (List[Set[Int]], List[Set[Int]]) = (List(), List())

  def execute: Unit = {
    //https://dl.acm.org/citation.cfm?id=3186125
    //Fractoid with the initial state for cliques
    val initialFractoid = fractalGraph.vfractoid.expand(1)

    val commStrategy = "scratch"
    val numPartitions = 1

    val testF = initialFractoid.
      //set("efilter", epredCallback(readyCliques)).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      set ("dump_path", dataPath).
      set ("top_N", N)
//    if (inc) {
//      testF.setNew(explorationSteps)
//    }

    val cliquesRes = testF.explore(explorationSteps)

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

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MaximalCliquesListing")

    val logLevel = "WARN"
    conf.set("spark.executor.memory", "16g")
    conf.set("spark.driver.memory","16g")
    conf.set("fractal.log.level", logLevel)
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //val (s, graphPath) = (22, "/Users/danielmuraveyko/Downloads/brock400-4/brock400-4.mtx")

    //val (s, graphPath) = (3, "/Users/danielmuraveyko/Desktop/els/for_kcore_0")
    //val (s, graphPath) = (4, "/Users/danielmuraveyko/Desktop/els2/00test.txt")
    //val (s, graphPath) = (3, "/Users/danielmuraveyko/Desktop/els2/01test.txt")
    //val (s, graphPath) = (4, "/Users/danielmuraveyko/Desktop/els2/02test.txt")
    val (s, graphPath) = (16, "/Users/danielmuraveyko/Desktop/els2/for_kcore_4")
    //val (s, graphPath) = (4800, "/Users/danielmuraveyko/Desktop/els2/for_kcore_1200")
    //val (s, graphPath) = (4800, "/Users/danielmuraveyko/Desktop/els2/127_test.txt")


    //val (s, graphPath) = (6000, "/Users/danielmuraveyko/Desktop/els2/for_kcore_1500")
    //val (s, graphPath) = (8000, "/Users/danielmuraveyko/Desktop/els2/for_kcore_2000")
    //val (s, graphPath) = (12000, "/Users/danielmuraveyko/Desktop/els2/for_kcore_3000")

    val time = System.currentTimeMillis()

    val sc = new SparkContext(conf)
    sc.setLogLevel(logLevel)

    val fc = new FractalContext(sc)

    logWarning(sc.uiWebUrl.getOrElse(""))

    val fractalGraph = fc.textFile(graphPath, graphClass = "br.ufmg.cs.systems.fractal.graph.EdgeListGraph")

    //TODO: if a graph can be colored with k colors, then the maximum clique in this graph must be smaller or equal to k
    //var explorationSteps = kcore.head

    val dataPath = "/Users/danielmuraveyko/maximal_cliques_listing/my_data/"


    var cliques : List[Set[Int]] = List()
    var cliquesIdx : List[Set[Int]] = List()

    val addCliques = (steps : Int, N : Int, inc : Boolean) => {
      val app = CliquesList(fractalGraph, steps, cliquesIdx, dataPath, N, inc)
      val (subgraphs, original_cliques) = app.findCliques()
      cliques = cliques ++ original_cliques
      cliquesIdx = cliquesIdx ++ subgraphs
    }

    val topN = 6

    addCliques(s, topN, true)

    logWarning(s"extends: ${KClistEnumerator.count}; dumps: ${KClistEnumerator.dumps}; loads: ${KClistEnumerator.loads}")
    logWarning(s"Time: ${(System.currentTimeMillis() - time) / 1000.0}s\n")

    for (r <- Refrigerator.result) {
      println(r)
      //println(r.size())
    }
    cleanDataFolder(dataPath)

    val f = new java.io.File("map.dat")
    f.delete()

    fc.stop()
    sc.stop()
  }

  def cleanDataFolder(path : String) : Unit = {
    val f = new java.io.File(path).listFiles()
    f.foreach(f => f.delete())
  }
}


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


//    def epredCallback(cliques : List[Set[Int]]) = {
//      new EdgeFilterFunc[EdgeInducedSubgraph] {
//        override def test(e: Edge[EdgeInducedSubgraph]): Boolean = {
//          !cliques.exists(c => c.contains(e.getSourceId) && c.contains(e.getDestinationId))
//        }
//      }
//    }

//    val vfilter = (v : VertexInducedSubgraph, c : Computation[VertexInducedSubgraph]) => {
//      true
//    }