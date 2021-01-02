package br.ufmg.cs.systems.fractal.computation

import java.io.{BufferedInputStream, BufferedOutputStream, File, FileInputStream, FileOutputStream}
import java.nio.file.{Files, Paths}
import java.util
import java.util.Arrays
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.function.IntConsumer

import akka.actor._
import br.ufmg.cs.systems.fractal.FractalSparkRunner
import br.ufmg.cs.systems.fractal.conf.SparkConfiguration
import br.ufmg.cs.systems.fractal.gmlib.clique.KClistEnumerator
import br.ufmg.cs.systems.fractal.graph.MainGraph
import br.ufmg.cs.systems.fractal.subgraph._
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList
import br.ufmg.cs.systems.fractal.util.{Logging, ProcessComputeFunc, SynchronizedNodeBuilder}
import breeze.linalg.max
import com.koloboke.collect.map.IntObjMap
import com.koloboke.collect.map.hash.HashIntObjMaps
import com.twitter.cassovary.graph.node.SynchronizedDynamicNode
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.util.{LongAccumulator, SizeEstimator}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.util.control.Breaks._
import scala.collection.immutable.HashMap
import scala.collection.mutable

/**
 * Underlying engine that runs the fractal master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */
class SparkFromScratchMasterEngine[S <: Subgraph](
                                                   _config: SparkConfiguration[S],
                                                   _parentOpt: Option[SparkMasterEngine[S]]) extends SparkMasterEngine[S] {

  import SparkFromScratchMasterEngine._

  def config: SparkConfiguration[S] = _config

  def parentOpt: Option[SparkMasterEngine[S]] = _parentOpt

  var masterActorRef: ActorRef = _

  def this(_sc: SparkContext, config: SparkConfiguration[S]) {
    this(config, None)
    sc = _sc
    init()
  }


  def this(_sc: SparkContext, config: SparkConfiguration[S],
           parent: SparkMasterEngine[S]) {
    this(config, Option(parent))
    sc = _sc
    init()
  }

  override def init(): Unit = {
    val start = System.currentTimeMillis

    super.init()

    // gtag computations must have incremental aggregations because we compute
    // from scratch all the steps, then if one of those depends on any previous
    // aggregation (e.g., fsm computation) we are safe
    config.set("incremental_aggregation", true)

    // gtag actor
    masterActorRef = ActorMessageSystem.createActor(this)

    logInfo(s"Started gtag-master-actor(step=${step}):" +
      s" ${masterActorRef}")

    val end = System.currentTimeMillis

    logInfo(s"${this} took ${(end - start)}ms to initialize.")
  }

  /**
   * Master's computation takes place here, superstep by superstep
   */
  lazy val next: Boolean = {


    logInfo(s"${this} Computation starting from ${stepRDD}," +
      s", StorageLevel=${stepRDD.getStorageLevel}")

    // save original container, i.e., without parents' computations
    val originalContainer = config.computationContainer[S]
    // we will contruct the pipeline in this var
    var cc = originalContainer.withComputationLabel("last_step_begins")
    // add parents' computations
    var curr: SparkMasterEngine[S] = this
    while (curr.parentOpt.isDefined) {
      curr = curr.parentOpt.get
      cc = curr.config.computationContainer[S].withComputationAppended(cc)
    }

    // configure custom ProcessComputeFunc and aggregations
    val processComputeFunc = getProcessComputeFunc()
    cc = {
      var nextComputationOpt = cc.nextComputationOpt
      var ccList: List[ComputationContainer[S]] = List.empty
      while (nextComputationOpt.isDefined) {
        nextComputationOpt = nextComputationOpt match {
          case Some(c) => if (c != null) {
            val cCast = c.asInstanceOf[ComputationContainer[S]]
            ccList = cCast :: ccList
            cCast.nextComputationOpt
          } else {
            None
          }
          case None => None
        }
      }

      var i = 0
      var prevComp: Option[ComputationContainer[S]] = None
      var ccListNew: List[ComputationContainer[S]] = List.empty
      while (i < ccList.size) {

        val c: ComputationContainer[S] = ccList.get(i)
        val computation = prevComp match {
          case Some(c0) =>
            c.shallowCopy(processComputeOpt = Option(processComputeFunc), nextComputationOpt = Option(c0), processOpt = None)
          case None =>
            c.shallowCopy(processComputeOpt = Option(processComputeFunc))
        }
        prevComp = Option(computation)

        ccListNew = computation :: ccListNew
        i += 1
      }
      cc = ccListNew.get(0).withComputationLabel("first_computation")
      cc.setDepth(0)
      cc
    }
    cc.setRoot()
    // set the modified pipelined computation
    this.config.set(SparkConfiguration.COMPUTATION_CONTAINER, cc)

    // NOTE: We need this extra initAggregations because this communication
    // strategy adds a 'previous_enumeration' aggregation
    cc.initAggregations(this.config)

    val initStart = System.currentTimeMillis
    val _configBc = configBc
    val mainGraphWasRead = this.config.isMainGraphRead
    stepRDD.mapPartitions { iter =>
      _configBc.value.initializeWithTag(isMaster = false)
      iter
    }.foreachPartition(_ => {})

    val initElapsed = System.currentTimeMillis - initStart

    logInfo(s"Initialization took ${initElapsed} ms")

    if (mainGraphWasRead) {
      val superstepStart = System.currentTimeMillis

      val enumerationStart = System.currentTimeMillis

      val _aggAccums = aggAccums

      val execEngines = getExecutionEngines(
        superstepRDD = stepRDD,
        superstep = step,
        configBc = configBc,
        aggAccums = _aggAccums,
        previousAggregationsBc = previousAggregationsBc)

      execEngines.foreachPartition(_ => {})

      val enumerationElapsed = System.currentTimeMillis - enumerationStart

      logInfo(s"Enumeration step=${step} took ${enumerationElapsed} ms")

      /** [1] We extract and aggregate the *aggregations* globally.
       */
      val aggregationsFuture = getAggregations(execEngines, numPartitions)
      // aggregations
      Await.ready(aggregationsFuture, atMost = Duration.Inf)
      aggregationsFuture.value.get match {
        case Success(previousAggregations) =>
          aggregations = mergeOrReplaceAggregations(aggregations,
            previousAggregations)

          aggregations.foreach { case (name, agg) =>
            val mapping = agg.getMapping
            val numMappings = agg.getNumberMappings
            logInfo(s"Aggregation[${name}][numMappings=${numMappings}][${agg}]\n" +
              s"${
                mapping.take(10).map(t => s"Aggregation[${name}][${step}]" + s" ${t._1}: ${t._2}").mkString("\n")
              }\n...")
          }

          previousAggregationsBc = sc.broadcast(aggregations)

        case Failure(e) =>
          logError(s"Error in collecting aggregations: ${e.getMessage}")
          throw e
      }

      execEngines.unpersist()

      logInfo(s"StorageLevel = ${storageLevel}")

      // whether the user chose to customize master computation, executed every
      // superstep
      masterComputation.compute()

      // print stats
      aggAccums.foreach { case (name, accum) =>
        logInfo(s"Accumulator[${step}][${name}]: ${accum.value}")
      }

      // master will send poison pills to all executor actors of this step
      masterActorRef ! Reset

      val superstepFinish = System.currentTimeMillis
      logInfo(
        s"Superstep $step finished in ${superstepFinish - superstepStart} ms"
      )
    }

    // make sure we maintain the engine's original state
    this.config.set(SparkConfiguration.COMPUTATION_CONTAINER, originalContainer)

    !sc.isStopped && !isComputationHalted
  }

  /**
   * Creates an RDD of execution engines
   * TODO
   */
  def getExecutionEngines[E <: Subgraph](
                                          superstepRDD: RDD[Unit],
                                          superstep: Int,
                                          configBc: Broadcast[SparkConfiguration[E]],
                                          aggAccums: Map[String, LongAccumulator],
                                          previousAggregationsBc: Broadcast[_]): RDD[SparkEngine[E]] = {

    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, cacheIter) =>

      configBc.value.initializeWithTag(isMaster = false)

      val execEngine = new SparkFromScratchEngine[E](
        partitionId = idx,
        step = superstep,
        accums = aggAccums,
        previousAggregationsBc = previousAggregationsBc,
        configurationId = configBc.value.getId
      )

      execEngine.init()
      execEngine.compute()
      execEngine.finalize()

      Iterator[SparkEngine[E]](execEngine)
    }

    execEngines
  }

  def getProcessComputeFunc(): ProcessComputeFunc[S] = {
    new ProcessComputeFunc[S] with Logging {

      var workStealingSys: WorkStealingSystem[S] = _

      var lastStepConsumer: LastStepConsumer[S] = _

      def apply(iter: SubgraphEnumerator[S], c: Computation[S]): ComputationResults[S] = {
        val config = c.getConfig
        KClistEnumerator.size = Refrigerator.size

        //        val t = iter.prefix.size() + iter.getDag.size()
        //        if (c.getDepth != 0 && t < Refrigerator.size) {
        //          if (iter.prefix.size != 0 && iter.getDag.size() == 0) {
        //            //TODO save clique?
        //            //logInfo(s"SAVING C ${iter.getDag} ${iter.prefix}")
        //          } else {
        //            //freeze
        //            //logInfo(s"ADDING C ${iter.getDag} ${iter.prefix}")
        //            //Refrigerator.addFrozenData(new FrozenDataHolder(iter.getDag, iter.prefix))
        //          }
        //          return new ComputationResults[S]
        //        }
        //        if (Refrigerator.freeze && c.getDepth == 0) {
        //          val pIter = Refrigerator.current.freezePrefix.iterator
        //          val dIter = Refrigerator.current.freezeDag.iterator
        //          val prefix = new IntArrayList
        //          val dag : IntObjMap[IntArrayList] = HashIntObjMaps.newMutableMap.asInstanceOf[IntObjMap[IntArrayList]]
        //
        //          while (pIter.hasNext) {
        //            prefix.add(pIter.next.asInstanceOf[Integer])
        //          }
        //
        //          while (dIter.hasNext) {
        //            val node = dIter.next
        //            dag.put(node.id, SynchronizedNodeBuilder.seq2ArrayList(node.outboundNodes))
        //          }
        //          iter.setForFrozen(prefix, dag)
        //        }

        if (c.getDepth == 0) {

          val execEngine = c.getExecutionEngine.
            asInstanceOf[SparkFromScratchEngine[S]]

          var currComp = c.nextComputation()

          while (currComp != null) {
            currComp.setExecutionEngine(execEngine)
            currComp.init(config)
            currComp.initAggregations(config)
            currComp = currComp.nextComputation
          }

          lastStepConsumer = new LastStepConsumer[S]()

          var start = System.currentTimeMillis
          logWarning("START COMPUTING")
          val start00 = System.currentTimeMillis

          val ret = processCompute(iter, c)
          logWarning(s"processCompute time: ${(System.currentTimeMillis - start00) / 1000.0}s; Found ${ret.getResults.size()} vertices.")

          val computationTree = new ComputationTree[S](c, null)

          for (r <- ret.getResults) {
            computationTree.adopt(new ComputationTree[S](computationTree, c.nextComputation(), r))
          }

          var result = computationTree
          var done = false
          var repeat = false
          val N = c.getConfig.getInteger("top_N", 1)
          while (!done) {

            val start0 = System.currentTimeMillis

            if (result != null && !repeat) {
              //visit first available child if possible
              val child = result.visit()
              if (child == null) {
                //well, we have no children, go back to parent
                result.killChildren()
                var back = true
                while (result.hasParent && back) {
                  result = result.parent
                  val child0 = result.visit()
                  if (child0 != null) {
                    result = child0
                    back = false
                  }
                }
                if (result.parent == null) {
                  //oh, this is init parent, we have visited all nodes
                  done = true
                }
              } else {
                //we go deeper, result is child
                result = child
              }
            }
            if (!done) {
//              if (result.head.vertex != -1) {
//                //Ok, we have only vertex, need to extend
//                val (next_iter, extend_time) = extend(iter, result.head.vertex)
//                val data_path = c.getConfig.getString("dump_path", "")
//                val (iterName, subName, ser_time) = save_iter(next_iter, iter.getSubgraph, data_path)
//                val orphan = new ComputationResult[S](iterName, subName)
//
//                logWarning("ONLY VERTEX " + result.head.vertex.toString +
//                  s" extend_time: ${extend_time / 1000.0}s; ser_time: ${ser_time / 1000.0}s;"
//                )
//
//                result = new ComputationTree[S](result, iter.getComputation.nextComputation(), orphan)
//              }

              if (result.head.serializedFileIter != "") {
                //Ok, we have serialized iter and sub
                val bis = new BufferedInputStream(new FileInputStream(result.head.serializedFileIter))
                val bArray = Stream.continually(bis.read).takeWhile(-1 !=).map(_.toByte).toArray
                result.head.enumerator = SparkConfiguration.deserialize[SubgraphEnumerator[S]](bArray)

                val subBis = new BufferedInputStream(new FileInputStream(result.head.serializedFileSub))
                val subbArray = Stream.continually(subBis.read).takeWhile(-1 !=).map(_.toByte).toArray
                result.head.subgraph = SparkConfiguration.deserialize[S](subbArray)
              }

              val subgraph = result.head.subgraph
              val iter = result.head.enumerator
              //iter.setForFrozen(subgraph, result.head.enumerator.getDag)

              if (subgraph.getVertices.size() == Refrigerator.size) {
                Refrigerator.result = subgraph.getVertices :: Refrigerator.result
                if (Refrigerator.result.size >= N) {
                  done = true
                }
                logWarning("FOUND!")
                repeat = false
              } else {
                val nextComp = result.nextComputation

                nextComp.setSubgraphEnumerator(iter)
                //nextComp.getSubgraphEnumerator.set(nextComp, subgraph)
                //nextComp.getSubgraphEnumerator.setForFrozen(result.head.enumerator.getDag)

                subgraph.nextExtensionLevel()
                logWarning(subgraph.toOutputString)

                val results = nextComp.compute(subgraph).getResults
               // logWarning(s"${subgraph.getVertices}: get ${results.size()} childrens")
                subgraph.previousExtensionLevel()

                if (false && results.length == 1) {
                  result.setHead(results.get(0))
                  result.updateId()
                  result.updateLevel()
                  // so here is the logic:
                  // on the previous iteration we checked all candidates, if the results.length == 1, we have only one candidate
                  // so, starting from this point we only need to find first candidate from next candidates
                  // because the number may only falling
                  nextComp.getSubgraphEnumerator.setGetFirstCandidate(true)
                  repeat = true
                } else {
                  if (results.length == 0) {
                    logWarning("|--> X")
                  }
                  for (orphan <- results) {
                    val c = new ComputationTree[S](result, nextComp.nextComputation(), orphan)
                    result.adopt(c)
                  }
                  repeat = false
                }


                val stepTime = System.currentTimeMillis - start0
                //logWarning(s"handling ${result.id}, level ${result.level}, time: ${stepTime / 1000.0}s")
                if (result.level % 100 == 0) {
                  logWarning(s"handling ${result.id}, level ${result.level}, adding ${results.length}, time: ${stepTime / 1000.0}s; " +
                    s"extend_time_all: ${extend_time_all / 1000.0}s; ser_time_all: ${ser_time_all / 1000.0}s; colors: ${colors_all / 1000.0}s;")
                }
                extend_time_all = 0
                ser_time_all = 0
                colors_all = 0

              }
            }
          }

          var elapsed = System.currentTimeMillis - start
          logInfo(s"WorkStealingMode internal=${config.internalWsEnabled()}" +
            s" external=${config.externalWsEnabled()}")
          logInfo(s"InitialComputation step=${c.getStep}" +
            s" partitionId=${c.getPartitionId} took ${elapsed} ms")

          if (false && config.wsEnabled()) {
            // setup work-stealing system
            start = System.currentTimeMillis

            def processComputeCallback(iter: SubgraphEnumerator[S], c: Computation[S]): ComputationResults[S] = {
              processCompute(iter, c)
            }

            val gtagExecutorActor = execEngine.slaveActorRef
            workStealingSys = new WorkStealingSystem[S](processComputeCallback, gtagExecutorActor, new ConcurrentLinkedQueue())
            workStealingSys.workStealingCompute(c)
            elapsed = System.currentTimeMillis - start

            logInfo(s"WorkStealingComputation step=${c.getStep}" +
              s" partitionId=${c.getPartitionId} took ${elapsed} ms")
          }
          val graph = config.getMainGraph[MainGraph[_, _]]()
          graph.closeMap()
          ret
        }

        else {
          processCompute(iter, c)
        }
      }

      var extend_time_all = 0L
      var ser_time_all = 0L
      var colors_all = 0L

      private def hasNextComputation(iter: SubgraphEnumerator[S], c: Computation[S], nextComp: Computation[S]): ComputationResults[S] = {
        //logWarning("hasNextComputation: " + iter.getSubgraph.getVertices.toString + " " + iter.getDag.keySet)
        var cou = 1

        val graph = c.getConfig.getMainGraph[MainGraph[_, _]]()
        val size = Refrigerator.size - 1
        val states = KClistEnumerator.getColors(graph)
        val result = new ComputationResults[S]
        val data_path = c.getConfig.getString("dump_path", "")
        val getOnlyFirst = iter.isGetFirstCandidate
        var found = false
        var extendNeeded = true

        while (iter.hasNext && !(found && getOnlyFirst)) {
          val u = iter.nextElem()
         // logWarning(u.toString)
          val prefixSize = iter.getSubgraph.getVertices.size()
          val maxPossibleSize = prefixSize + max(0, iter.getAdditionalSize - 1)

          val (uniqColors, elapsed) = FractalSparkRunner.time {
            val dag = iter.getDag

            val neigh_colors = ListBuffer.empty[Int]
            neigh_colors += states(u)

            if (!dag.containsKey(u)) {
              val neighbours = graph.getVertexNeighbours(u)
              val cursor = neighbours.getInternalSet.cursor()
              while (cursor.moveNext()) {
                neigh_colors += states(cursor.elem())
              }
            } else {
              val dagNeighbours = dag.get(u)
              val cursor = dagNeighbours.cursor()
              while (cursor.moveNext()) {
                neigh_colors += states(cursor.elem())
              }
            }
            //k-clique contains k colors
            neigh_colors.distinct.size
          }

          colors_all += elapsed

          val isFirstComputation = maxPossibleSize == 0
          val isSizeOk = !(isFirstComputation && uniqColors < size || !isFirstComputation && maxPossibleSize < size)

          if (isSizeOk && uniqColors + prefixSize > size) {
            found = true

            if (prefixSize == 0) {
            //  extendNeeded = false
            //  KClistEnumerator.writeSizes = true
              Refrigerator.graphCounter += 1
            }

            if (extendNeeded) {
              val (next_iter, extend_time) = extend(iter, u)
              if (!getOnlyFirst) {
                val s = iter.getSubgraph
                logWarning("|--> " + s.toOutputString)

                val (iterName, subName, ser_time) = save_iter(next_iter, iter.getSubgraph, data_path)

                result.add(iterName, subName)
//
//                logWarning("DUMP TO FILE! " + cou.toString + " " + iter.getSubgraph.getVertices.toString + " " +
//                  next_iter.getDag.keySet + ""
//                  //s" extend_time: ${extend_time / 1000.0}s; ser_time: ${ser_time / 1000.0}s; get colors: ${elapsed / 1000.0}s;"
//                )
              } else {
                iter.shouldRemoveLastWord = false
                result.add(next_iter, iter.getSubgraph)
               // logWarning(s"Only add vertex: ${u}"+ " " + iter.getSubgraph.getVertices.toString)
                //logWarning(s"extend_time: ${extend_time / 1000.0}s; get colors: ${elapsed / 1000.0}s;")
              }
            } else {
              //result.add(u)
            }

            cou += 1

          }
        }
        //KClistEnumerator.writeSizes = false
        result
      }

      private def extend(iter: SubgraphEnumerator[S], u : Int): (SubgraphEnumerator[S], Long) = {
        val time0 = System.currentTimeMillis
        val next_iter = iter.extend(u)
        val extend_time = System.currentTimeMillis - time0
        extend_time_all += extend_time
        (next_iter, extend_time)
      }

      private def save_iter(next_iter: SubgraphEnumerator[S], subgraph: Subgraph, data_path : String): (String, String, Long)  = {
        val ser = System.currentTimeMillis

        val iterB = SparkConfiguration.serialize(next_iter)
        val subB = SparkConfiguration.serialize(subgraph)
        val iterFile = File.createTempFile("iter", "", new File(data_path))
        val subFile = File.createTempFile("sub", "", new File(data_path))
        Files.write(Paths.get(iterFile.getCanonicalPath), iterB)
        Files.write(Paths.get(subFile.getCanonicalPath), subB)

        val ser_time = System.currentTimeMillis - ser
        ser_time_all += ser_time

        (iterFile.getCanonicalPath, subFile.getCanonicalPath, ser_time)
      }

      private def lastComputation(iter: SubgraphEnumerator[S], c: Computation[S]): ComputationResults[S] = {
        val WRITE_TO_FILE = false

        var addWords = 0L
        var subgraphsGenerated = 0L

        val wordIds = iter.getWordIds
        val result = new ComputationResults[S]

        if (wordIds != null) {
          KClistEnumerator.count += 1

          if (WRITE_TO_FILE) {
            lastStepConsumer.set(iter.getSubgraph, c)
            wordIds.forEach(lastStepConsumer)
            addWords += lastStepConsumer.addWords
            subgraphsGenerated += lastStepConsumer.subgraphsGenerated
          } else {
            val subgrap_bytes = SparkConfiguration.serialize(iter.getSubgraph)
            val new_iter = SparkConfiguration.deserialize[SubgraphEnumerator[S]](SparkConfiguration.serialize(iter))

            val keys = iter.getDag.keySet
            new_iter.getDag.clear()

            for (w <- keys) {
              val new_subgraph = SparkConfiguration.deserialize[S](subgrap_bytes)
              new_subgraph.addWord(w)
              //TODO: save it!
              result.add(new_iter, new_subgraph)
            }
          }
        } else {
          val subgraph = iter.next()
          addWords += 1
          if (c.filter(subgraph)) {
            subgraphsGenerated += 1
            c.process(subgraph)
          }
        }

        result
      }

      private def processCompute(iter: SubgraphEnumerator[S], c: Computation[S]): ComputationResults[S] = {
        val nextComp = c.nextComputation()

        if (nextComp != null) {
          hasNextComputation(iter, c, nextComp)
        } else {
          lastComputation(iter, c)
        }
      }
    }
  }

}

class LastStepConsumer[E <: Subgraph] extends IntConsumer {
  var subgraph: E = _
  var computation: Computation[E] = _
  var addWords: Long = _
  var subgraphsGenerated: Long = _

  def set(subgraph: E, computation: Computation[E]): LastStepConsumer[E] = {
    this.subgraph = subgraph
    this.computation = computation
    this.addWords = 0L
    this.subgraphsGenerated = 0L
    this
  }

  override def accept(w: Int): Unit = {
    addWords += 1
    subgraph.addWord(w)
    if (computation.filter(subgraph)) {
      subgraphsGenerated += 1
      //print to file, may be useful
      computation.process(subgraph)
    }
    subgraph.removeLastWord()
  }
}

object SparkFromScratchMasterEngine {
  val NEIGHBORHOOD_LOOKUPS = "neighborhood_lookups"

  val NEIGHBORHOOD_LOOKUPS_ARR = {
    val arr = new Array[String](16)
    var i = 0
    while (i < arr.length) {
      arr(i) = s"${NEIGHBORHOOD_LOOKUPS}_${i}"
      i += 1
    }
    arr
  }

  def NEIGHBORHOOD_LOOKUPS(depth: Int): String = {
    NEIGHBORHOOD_LOOKUPS_ARR(depth)
  }

  val CANONICAL_SUBGRAPHS = "canonical_subgraphs"
  val VALID_SUBGRAPHS = "valid_subgraphs"
  val AGG_CANONICAL_FILTER = "canonical_filter"
}
