package br.ufmg.cs.systems.fractal

import br.ufmg.cs.systems.fractal.graph.{Edge, Vertex}
import br.ufmg.cs.systems.fractal.subgraph.{EdgeInducedSubgraph, VertexInducedSubgraph}
import br.ufmg.cs.systems.fractal.util.{EdgeFilterFunc, Logging, VertexFilterFunc}
import org.apache.hadoop.io._
import org.apache.spark.{SparkConf, SparkContext}

trait FractalSparkApp extends Logging {
  def fractalGraph: FractalGraph
  def execute: Unit
}

class VSubgraphsApp(val fractalGraph: FractalGraph,
                    commStrategy: String,
                    numPartitions: Int,
                    explorationSteps: Int) extends FractalSparkApp {
  def execute: Unit = {
    val vsubgraphsRes = fractalGraph.vfractoidAndExpand.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore (explorationSteps)

    vsubgraphsRes.compute()
  }
}

class MotifsApp(val fractalGraph: FractalGraph,
                commStrategy: String,
                numPartitions: Int,
                explorationSteps: Int) extends FractalSparkApp {
  def execute: Unit = {
    val motifsRes = fractalGraph.motifs.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      motifsRes.compute()
    }

    logInfo (s"MotifsApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${motifsRes.numValidSubgraphs()} elapsed=${elapsed}"
    )
  }
}

class CliquesOptApp(val fractalGraph: FractalGraph,
                    commStrategy: String,
                    numPartitions: Int,
                    explorationSteps: Int) extends FractalSparkApp {
  def execute: Unit = {

    var s : Set[Int] = Set(39,129,180,138,11,14,24,185,201,63,215,17,204,12,38,203,228,94,192,100,217,112,139,77,131,59,
      113,118,40,186,96,81,82,216,28,29,98,222,30,110,214,83,49,78,109,178,218,84,122,115,187,76,243,182,208,73,181,227,
      237,221,7,8,223,88,244,34,4,134,193,80,105,65,35,104,111,175,202,74,238,18,236,103,199,99,224,41,53,46,213,220,62,
      33,6,69,3,135,123,26,5,13,52,189,87,119,239,179,64,75,188,133,205,45,132,70,183,108,10,43,61,47,97,116,48,117,240,
      27,0,42,210,209,234,68,170,146,140,168,263,275,252,251,248,144,274,164,259,143,174,153,157,152,279,269,253,169,158,
      262,154,273,148,258,245,249,257,147,255,17,167,151,150,272,278,256,145,166,250,271,173,19,34)

    val vpred = new VertexFilterFunc[VertexInducedSubgraph] {
      override def test(v: Vertex[VertexInducedSubgraph]): Boolean = {
        //  v.getVertexLabel() < 170
        //true;
        !s.contains(v.getVertexId)
      }
    }

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

    val (accums, elapsed) = FractalSparkRunner.time {
      cliquesRes.compute()
    }

    logInfo (s"CliquesOptApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${cliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
      )

    cliquesRes.saveSubgraphsAsTextFile("/Users/danielmuraveyko/Desktop/test")
  }
}

class CliquesApp(val fractalGraph: FractalGraph,
                 commStrategy: String,
                 numPartitions: Int,
                 explorationSteps: Int) extends FractalSparkApp {
  def execute: Unit = {
    val cliquesRes = fractalGraph.cliques.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      set ("fractal.optimizations", "br.ufmg.cs.systems.fractal.optimization.CliqueOptimization").
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      cliquesRes.compute()
    }

    logInfo (s"CliquesApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${cliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
      )
  }
}

class MaximalCliquesApp(val fractalGraph: FractalGraph,
                        commStrategy: String,
                        numPartitions: Int,
                        explorationSteps: Int) extends FractalSparkApp {
  def execute: Unit = {
    val maximalcliquesRes = fractalGraph.maximalcliques.
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (counting, elapsed) = FractalSparkRunner.time {
      maximalcliquesRes.compute()
    }

    logInfo (s"MaximalCliquesApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${maximalcliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
    )

  //  maximalcliquesRes.saveSubgraphsAsTextFile("/Users/danielmuraveyko/Desktop/maximalcliques")
  }
}

class QuasiCliquesApp(val fractalGraph: FractalGraph,
                      commStrategy: String,
                      numPartitions: Int,
                      explorationSteps: Int,
                      minDensity: Double) extends FractalSparkApp {
  def execute: Unit = {
    val quasiCliquesRes = fractalGraph.quasiCliques(explorationSteps, minDensity).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions)

    val (counting, elapsed) = FractalSparkRunner.time {
      quasiCliquesRes.compute()
    }

    logInfo (s"QuasiCliquesApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${quasiCliquesRes.numValidSubgraphs()} elapsed=${elapsed}"
    )
  }
}

class FSMApp(val fractalGraph: FractalGraph,
             commStrategy: String,
             numPartitions: Int,
             explorationSteps: Int,
             support: Int) extends FractalSparkApp {
  def execute: Unit = {
    fractalGraph.set ("comm_strategy", commStrategy)
    fractalGraph.set ("num_partitions", numPartitions)

    val (fsm, elapsed) = FractalSparkRunner.time {
      fractalGraph.fsm(support, explorationSteps)
    }

    logInfo (s"FSMApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${fsm.numValidSubgraphs()} elapsed=${elapsed}"
    )
  }
}

class KeywordSearchApp(val fractalGraph: FractalGraph,
                       commStrategy: String,
                       numPartitions: Int,
                       explorationSteps: Int,
                       queryWords: Array[String]) extends FractalSparkApp {
  def execute: Unit = {
    val (kws, elapsed) = FractalSparkRunner.time {
      fractalGraph.keywordSearch(numPartitions, queryWords)
    }

    logInfo (s"KeywordSearchApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} " +
      s" numValidSubgraphs=${kws.numValidSubgraphs()} elapsed=${elapsed}"
    )
  }
}

class GQueryingApp(val fractalGraph: FractalGraph,
                   commStrategy: String,
                   numPartitions: Int,
                   explorationSteps: Int,
                   subgraphPath: String) extends FractalSparkApp {
  def execute: Unit = {

    val subgraph = new FractalGraph(
      subgraphPath, fractalGraph.fractalContext, "warn")

    val gquerying = fractalGraph.gquerying(subgraph).
      set ("comm_strategy", commStrategy).
      set ("num_partitions", numPartitions).
      explore(explorationSteps)

    val (accums, elapsed) = FractalSparkRunner.time {
      gquerying.compute()
    }

    println (s"GQueryingApp comm=${commStrategy}" +
      s" numPartitions=${numPartitions} explorationSteps=${explorationSteps}" +
      s" graph=${fractalGraph} subgraph=${subgraph}" +
      s" counting=${gquerying.numValidSubgraphs()} elapsed=${elapsed}"
      )
  }
}

object FractalSparkRunner {
  def time[R](block: => R): (R, Long) = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    (result, t1 - t0)
  }

  def main(args : Array[String]): Unit = {
   main3(args);
  }

  def main3(args: Array[String]) {
    // args
    var i = 0
    val graphClass = "br.ufmg.cs.systems.fractal.graph.BasicMainGraph"

    i += 1
    //    val graphPath = "/Users/danielmuraveyko/fractal/data/cliques_dm.graph"
    //    20,4
    //    4,119
    //    5,46
    //    6,40
    //    7,4
    //    8,18
    //    9,9
    //    13,3
    val graphPath = "/Users/danielmuraveyko/Desktop/el_6.csv"
    i += 1
    val algorithm = "cliquesopt"
    i += 1
    val commStrategy = "scratch"
    i += 1
    val numPartitions = 1
    i += 1
    val explorationSteps = 19
    i += 1
    val logLevel = "info"

    val conf = new SparkConf().setMaster("local[4]").setAppName("MyMotifsApp")
    val sc = new SparkContext(conf)

    if (!sc.isLocal) {
      // TODO: this is ugly but have to make sure all spark executors are up by
      // the time we start executing fractal applications
      Thread.sleep(10000)
    }

    val fc = new FractalContext(sc, logLevel)
    val fractalGraph = fc.textFile (graphPath, graphClass = graphClass)

    val app = algorithm.toLowerCase match {
      case "vsubgraphs" =>
        new VSubgraphsApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "motifs" =>
        new MotifsApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "cliques" =>
        new CliquesApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "cliquesopt" =>
        new CliquesOptApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "maximalcliques" =>
        new MaximalCliquesApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "quasicliques" =>
        i += 1
        val minDensity = args(i).toDouble
        new QuasiCliquesApp(fractalGraph, commStrategy, numPartitions,
          explorationSteps, minDensity)
      case "fsm" =>
        i += 1
        val support = args(i).toInt
        new FSMApp(fractalGraph, commStrategy, numPartitions,
          explorationSteps, support)
      case "kws" =>
        i += 1
        val queryWords = args.slice(i, args.length)
        new KeywordSearchApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps, queryWords)
      case "gquerying" =>
        i += 1
        val subgraphPath = args(i)
        new GQueryingApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps, subgraphPath)
      case appName =>
        throw new RuntimeException(s"Unknown app: ${appName}")
    }

    i += 1
    while (i < args.length) {
      println (s"Found config=${args(i)}")
      val kv = args(i).split(":")
      if (kv.length == 2) {
        fractalGraph.set (kv(0), kv(1))
      }
      i += 1
    }

    app.execute

    fc.stop()
    sc.stop()
  }

  def main2(args: Array[String]) {
    // args
    var i = 0
    val graphClass = args(i) match {
      case "al" =>
        "br.ufmg.cs.systems.fractal.graph.BasicMainGraph"
      case "el" =>
        "br.ufmg.cs.systems.fractal.graph.EdgeListGraph"
      case "al-kws" =>
        "br.ufmg.cs.systems.fractal.gmlib.keywordsearch.KeywordSearchGraph"
      case other =>
        throw new RuntimeException(s"Input graph format '${other}' is invalid")
    }
    i += 1
    val graphPath = args(i)
    i += 1
    val algorithm = args(i)
    i += 1
    val commStrategy = args(i)
    i += 1
    val numPartitions = args(i).toInt
    i += 1
    val explorationSteps = args(i).toInt
    i += 1
    val logLevel = args(i)

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    if (!sc.isLocal) {
      // TODO: this is ugly but have to make sure all spark executors are up by
      // the time we start executing fractal applications
      Thread.sleep(10000)
    }

    val fc = new FractalContext(sc, logLevel)
    val fractalGraph = fc.textFile (graphPath, graphClass = graphClass)

    val app = algorithm.toLowerCase match {
      case "vsubgraphs" =>
        new VSubgraphsApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "motifs" =>
        new MotifsApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "cliques" =>
        new CliquesApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "cliquesopt" =>
        new CliquesOptApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "maximalcliques" =>
        new MaximalCliquesApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps)
      case "quasicliques" =>
        i += 1
        val minDensity = args(i).toDouble
        new QuasiCliquesApp(fractalGraph, commStrategy, numPartitions,
          explorationSteps, minDensity)
      case "fsm" =>
        i += 1
        val support = args(i).toInt
        new FSMApp(fractalGraph, commStrategy, numPartitions,
          explorationSteps, support)
      case "kws" =>
        i += 1
        val queryWords = args.slice(i, args.length)
        new KeywordSearchApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps, queryWords)
      case "gquerying" =>
        i += 1
        val subgraphPath = args(i)
        new GQueryingApp(fractalGraph, commStrategy,
          numPartitions, explorationSteps, subgraphPath)
      case appName =>
        throw new RuntimeException(s"Unknown app: ${appName}")
    }

    i += 1
    while (i < args.length) {
      println (s"Found config=${args(i)}")
      val kv = args(i).split(":")
      if (kv.length == 2) {
        fractalGraph.set (kv(0), kv(1))
      }
      i += 1
    }

    app.execute

    fc.stop()
    sc.stop()
  }
}
