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
import com.koloboke.collect.map.{IntObjCursor, IntObjMap}
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
          val N = c.getConfig.getInteger("top_N", 1)
          val graph = c.getConfig.getMainGraph[MainGraph[_, _]]()

          var foundedCliques = Refrigerator.result.size
          var first = true

          val repeatOrClean = () => {
            var continue = true
            if (first) {
              first = false
            } else {
              if (Refrigerator.result.size == N) {
                continue = false
              } else if (Refrigerator.result.size > foundedCliques) {
                //aha, we got new cliques!
                foundedCliques = Refrigerator.result.size

                val start = System.currentTimeMillis
                graph.removeCliques(Refrigerator.result)
                logWarning(s"removeCliques time: ${(System.currentTimeMillis - start) / 1000.0}s;")

                KClistEnumerator.dropColors()
              } else {
                //well, there are no cliques, reduce clique size and try again
                Refrigerator.inc()

              }
            }

            KClistEnumerator.getColors(graph)
            Refrigerator.neigh_sizes = KClistEnumerator.neigboursSizes
            Refrigerator.size = Refrigerator.neigh_sizes.get(Refrigerator.neigh_sizes.size - Refrigerator.idx) + 1
            logWarning("Refrigerator.size: " + Refrigerator.size.toString)
            KClistEnumerator.size = Refrigerator.size

            if (Refrigerator.size < 4) {
              continue = false
            }

            iter.resetCursor()
            continue
          }

          while (repeatOrClean()) {
            val execEngine = c.getExecutionEngine.asInstanceOf[SparkFromScratchEngine[S]]
            var currComp = c.nextComputation()
            while (currComp != null) {
              currComp.setExecutionEngine(execEngine)
              currComp.init(config)
              currComp.initAggregations(config)
              currComp = currComp.nextComputation
            }
            lastStepConsumer = new LastStepConsumer[S]()

            val start = System.currentTimeMillis

            val ret = processCompute(iter, c)
            logWarning(s"processCompute time: ${(System.currentTimeMillis - start) / 1000.0}s; Found ${ret.getResults.size()} vertices.")

            val computationTree = new ComputationTree[S](c, null)

            for (r <- ret.getResults) {
              computationTree.adopt(new ComputationTree[S](computationTree, c.nextComputation(), r))
            }

            var result = computationTree
            var done = false
            var repeat = false

            val addClique = (s : S) => {
              Refrigerator.result = s.getVertices :: Refrigerator.result
              if (Refrigerator.result.size >= N) {
                done = true
              }
              repeat = false
              logWarning("FOUND!")
            }

            val repeatLoop = () => {
              if (done) {
                false
              } else if (result != null && !repeat) {
                val (r, d) = next_children(result)
                result = r
                done = d
                !done
              } else {
                true
              }
            }

            while (repeatLoop()) {

              val start0 = System.currentTimeMillis

              if (!done && !repeat) {
                if (result.head.getResultType == ResultType.SERIALIZED) {
                  //Ok, we have serialized iter and sub
                  val (e, s, ser_time) = read_iter(result.head.serializedFileIter, result.head.serializedFileSub, c)
                  result.head.enumerator = e
                  result.head.subgraph = s
                  KClistEnumerator.loads += 1
                  logWarning(s"deser iter: ${result.id}; size: ${s.getVertices.size}; dag size: ${e.getDag.size}; deser time: ${ser_time / 1000.0}s; ")
                } else if (result.head.getResultType == ResultType.VERTEX) {
                  //Ok, we have only vertex, need to extend
                  iter.maybeRemoveLastWord()
                  val (next_iter, extend_time) = extend(iter, result.head.vertex)
                  val ser = System.currentTimeMillis
                  val (iterNew, subgraph) = copyIter(next_iter, iter.getSubgraph)
                  val ser_time = System.currentTimeMillis - ser
                  val orphan = new ComputationResult[S](iterNew, subgraph)
                  logWarning("one vertex " + result.head.vertex.toString + s" extend_time: ${extend_time / 1000.0}s; copy iter: ${ser_time / 1000.0}s;")
                  result = new ComputationTree[S](result, iter.getComputation.nextComputation(), orphan)
                } else if (result.head.getResultType == ResultType.SUBGRAPH) {
                  //Ok, we have subgraph, rebuild iter
                  val (e, time) = sub2iter(c, result.head.subgraph)
                  result.head.enumerator = e
                  logWarning(s"rebuild iter: ${result.id}; size: ${result.head.subgraph.getVertices.size}; dag size: ${e.getDag.size}; rebuild time: ${time / 1000.0}s; ")
                }


              }

              val subgraph = result.head.subgraph
              val saved_iter = result.head.enumerator

              if (subgraph.getVertices.size() == Refrigerator.size) {
                addClique(subgraph)
              } else {
                if (writePath) {
                  logWarning(subgraph.toOutputString + " -- dag size: " + saved_iter.getDag.size().toString)
                }

                val nextComp = result.nextComputation
                nextComp.setSubgraphEnumerator(saved_iter)
                subgraph.nextExtensionLevel()
                val results = nextComp.compute(subgraph).getResults
                subgraph.previousExtensionLevel()

                if (results.length == 1) {
                  // so here is the logic:
                  // on the previous iteration we checked all candidates, if the results.length == 1, we have only one candidate
                  // so, starting from this point we only need to find first candidate from next candidates
                  // because the number may only falling
                  result.setHead(results.get(0))
                  result.updateId()
                  result.updateLevel()

                  repeat = true

//---------------- EXPERIMENTAL PART ----------------
                  val s = result.head.subgraph
                  val dag = result.head.enumerator.getDag
                  if (s.getVertices.size + dag.keySet().size() == Refrigerator.size) {
                    if (KClistEnumerator.isClique(dag)) {
                      logWarning(s"KClistEnumerator.isClique for ${s.getVertices}!")

                      for (i <- dag.keySet()) {
                        s.addWord(i)
                      }
                      addClique(s)
                    }
                  }
//---------------------------------------------------

                } else {
                  if (writePath && results.isEmpty) {
                    logWarning("|--> X")
                  }

                  result.head.reset()

                  for (orphan <- results) {
                    val c = new ComputationTree[S](result, nextComp.nextComputation(), orphan)

                    result.adopt(c)
                  }
                  repeat = false
                }

                val stepTime = System.currentTimeMillis - start0
                if (result.level % 100 == 0) {
                  logWarning(s"handling ${result.id}, level ${result.level}, " +
                    s"time: ${stepTime / 1000.0}s; " +
                    s"extend_time_all: ${extend_time_all / 1000.0}s; " +
                    s"ser_time_all: ${ser_time_all / 1000.0}s; colors: ${colors_all / 1000.0}s;")

                  //colors_vertices_all: ${colors_vertices_all}; colors_neighbours_all: ${colors_neighbours_all};")
                }
                extend_time_all = 0
                ser_time_all = 0
                colors_all = 0
                colors_vertices_all = 0L
                colors_neighbours_all = 0L
              }
            }

            val elapsed = System.currentTimeMillis - start
            logInfo(s"WorkStealingMode internal=${config.internalWsEnabled()}" +
              s" external=${config.externalWsEnabled()}")
            logInfo(s"InitialComputation step=${c.getStep}" +
              s" partitionId=${c.getPartitionId} took ${elapsed} ms")

            //          if (false && config.wsEnabled()) {
            //            // setup work-stealing system
            //            start = System.currentTimeMillis
            //
            //            def processComputeCallback(iter: SubgraphEnumerator[S], c: Computation[S]): ComputationResults[S] = {
            //              processCompute(iter, c)
            //            }
            //
            //            val gtagExecutorActor = execEngine.slaveActorRef
            //            workStealingSys = new WorkStealingSystem[S](processComputeCallback, gtagExecutorActor, new ConcurrentLinkedQueue())
            //            workStealingSys.workStealingCompute(c)
            //            elapsed = System.currentTimeMillis - start
            //
            //            logInfo(s"WorkStealingComputation step=${c.getStep}" +
            //              s" partitionId=${c.getPartitionId} took ${elapsed} ms")
            //          }

            ret
          }

          graph.closeMap()
//TODO
          null
        } else {
          processCompute(iter, c)
        }
      }

      var extend_time_all = 0L
      var ser_time_all = 0L
      var colors_all = 0L
      var colors_vertices_all = 0L
      var colors_neighbours_all = 0L
      val writePath = true

      private def hasNextComputation(iter: SubgraphEnumerator[S], c: Computation[S], nextComp: Computation[S]): ComputationResults[S] = {
        val graph = c.getConfig.getMainGraph[MainGraph[_, _]]()
        val states = KClistEnumerator.getColors(graph)
        val size = Refrigerator.size - 1

        val result = new ComputationResults[S]
        val data_path = c.getConfig.getString("dump_path", "")
        val getOnlyFirst = iter.isGetFirstCandidate
        var found = false
        var extendNeeded = true

        var firstIter: Array[Byte] = null
        var firstSub: Array[Byte] = null
        var firstIterSaved = false
        var next_iter: SubgraphEnumerator[S] = null

        while (!(found && getOnlyFirst) && iter.hasNext) {
          val u = iter.nextElem()

          val prefixSize = iter.getSubgraph.getVertices.size()
          val maxPossibleSize = prefixSize + max(0, iter.getAdditionalSize - 1)

          val (uniqColors, elapsed) = FractalSparkRunner.time {
            val dag = iter.getDag

            val neigh_colors = ListBuffer.empty[Int]
            neigh_colors += states(u)

            if (!dag.containsKey(u)) {
              val neighbours = graph.getVertexNeighbours(u)
              val cursor = neighbours.cursor()
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
            colors_neighbours_all += neigh_colors.size
            //k-clique contains k colors
            neigh_colors.distinct.size
          }
          colors_vertices_all += 1
          colors_all += elapsed

          val isFirstComputation = maxPossibleSize == 0
          val isSizeOk = !(isFirstComputation && uniqColors < size || !isFirstComputation && maxPossibleSize < size)

          if (isSizeOk && uniqColors + prefixSize > size) {
            found = true

            if (prefixSize == 0) {
              extendNeeded = false
              Refrigerator.graphCounter += 1
            }

            var ser: Long = 0

            if (extendNeeded) {
              var wrote = false
              val (n_i, extend_time) = extend(iter, u)

              if (writePath) {
                val str = if (getOnlyFirst) "only first " else ""
                logWarning("|--> " + str + iter.getSubgraph.toOutputString)
              }
              next_iter = n_i

              if (!getOnlyFirst) {
                if (!firstIterSaved && firstIter == null) {
                  iter.extend = false
                  val time0 = System.currentTimeMillis

                  val (i, s) = iter2bytes(next_iter, iter.getSubgraph)
                  firstIter = i
                  firstSub = s

                  ser = System.currentTimeMillis - time0
                  wrote = true
                }

                if (!(wrote || getOnlyFirst)) {
                  var ser_time0_first : Long = 0
                  val savedFirst = if (firstIter != null) "+saved first; " else ""
                  val extended = if (iter.extend) "" else "+no extend; "

                  if (firstIter != null) {

                    if (iter.getSubgraph.getNumVertices > KClistEnumerator.EXTENDS_THRESHOLD) {
                      //todo no need
                      val (firstIterDeser, firstSubDeser) = bytes2iter(firstIter, firstSub)

                      val (iterName0, subName0, ser_time0) = save_iter(firstIterDeser, firstSubDeser, data_path)
                      result.add(iterName0, subName0)
                      ser_time0_first = ser_time0
                    } else {
                      val subgraph = SparkConfiguration.deserialize[S](firstSub)
                      result.add(subgraph)
                    }
                    firstIterSaved = true
                    firstIter = null
                  }

                  var ser_time: Long = 0
                  var dump_msg = ""
                  if (iter.getSubgraph.getNumVertices > KClistEnumerator.EXTENDS_THRESHOLD) {
                    val (iterName, subName, s) = save_iter(next_iter, iter.getSubgraph, data_path)
                    ser_time = s
                    result.add(iterName, subName)
                    KClistEnumerator.dumps += 1
                    dump_msg = s"dump to file ${KClistEnumerator.dumps.toString} "
                  } else {
                    val subB = SparkConfiguration.serialize(iter.getSubgraph)
                    val subgraph = SparkConfiguration.deserialize[S](subB)
                    result.add(subgraph)

                    dump_msg = s"save sub "
                  }

                  ser_time_all += (ser_time + ser_time0_first + ser)

                  logWarning(
                    s"$dump_msg$savedFirst$extended" +
                    s"dag size: ${iter.getDag.size}; sub size: ${iter.getSubgraph.getVertices.size()}" +
                    s"; extend_time: ${extend_time / 1000.0}s; ser_time: " +
                    s"${ser_time / 1000.0}s; get colors: ${elapsed / 1000.0}s;"
                  )
                }
              }
            } else {
              result.add(u)
            }
          }
        }

        if (result.size() == 0 && next_iter != null) {
          val time0 = System.currentTimeMillis

          next_iter.shouldRemoveLastWord = false
          next_iter.setGetFirstCandidate(true)

          if (getOnlyFirst) {
            result.add(next_iter, iter.getSubgraph)
          } else {
            //iter.hasNext removes the added vertex, so we have to copy it :(
            val firstSubDeser = SparkConfiguration.deserialize[S](firstSub)
            result.add(next_iter, firstSubDeser)
          }

          val deser = System.currentTimeMillis - time0

          //logWarning(s"copy iter: ${deser / 1000.0}s;")
        }
        iter.extend = true
        result
      }

      private def extend(iter: SubgraphEnumerator[S], u: Int): (SubgraphEnumerator[S], Long) = {
        val time0 = System.currentTimeMillis
        val next_iter = iter.extend(u)
        val extend_time = System.currentTimeMillis - time0
        extend_time_all += extend_time
        (next_iter, extend_time)
      }

      private def save_iter(next_iter: SubgraphEnumerator[S], subgraph: S, data_path: String): (String, String, Long) = {

        val ser = System.currentTimeMillis

        val (iterB, subB) = iter2bytes(next_iter, subgraph)

        val iterFile = File.createTempFile("iter", "", new File(data_path))
        val subFile = File.createTempFile("sub", "", new File(data_path))
        //KClistEnumerator.addIter(iterFile.getCanonicalPath, iterB)
        //KClistEnumerator.addIter(subFile.getCanonicalPath, subB)

        Files.write(Paths.get(iterFile.getCanonicalPath), iterB)
        Files.write(Paths.get(subFile.getCanonicalPath), subB)

        val ser_time = System.currentTimeMillis - ser
        ser_time_all += ser_time

        (iterFile.getCanonicalPath, subFile.getCanonicalPath, ser_time)
      }

      private def iter2bytes(next_iter: SubgraphEnumerator[S], subgraph: S): (Array[Byte], Array[Byte]) = {
        val iterB = SparkConfiguration.serialize(next_iter)
        val subB = SparkConfiguration.serialize(subgraph)
        (iterB, subB)
      }

      private def bytes2iter(iterB: Array[Byte], subB: Array[Byte]): (SubgraphEnumerator[S], S) = {
        val iter = SparkConfiguration.deserialize[SubgraphEnumerator[S]](iterB)
        val subgraph = SparkConfiguration.deserialize[S](subB)

        (iter, subgraph)
      }

      private def copyIter(next_iter: SubgraphEnumerator[S], subgraph: S): (SubgraphEnumerator[S], S) = {
        val (iterB, subB) = iter2bytes(next_iter, subgraph)
        bytes2iter(iterB, subB)
      }

      private def read_iter(iter_path: String, sub_path: String, computation: Computation[S]): (SubgraphEnumerator[S], S, Long) = {
        val ser = System.currentTimeMillis
        //if (iter_path != "") {
        val bis = new BufferedInputStream(new FileInputStream(iter_path))
        val subBis = new BufferedInputStream(new FileInputStream(sub_path))

        val bArray = Stream.continually(bis.read).takeWhile(-1 !=).map(_.toByte).toArray
        val subbArray = Stream.continually(subBis.read).takeWhile(-1 !=).map(_.toByte).toArray

        //val subbArray = KClistEnumerator.getIter(sub_path)
        //val bArray = KClistEnumerator.getIter(iter_path)

        val (iter, subgraph) = bytes2iter(bArray, subbArray)

        val ser_time = System.currentTimeMillis - ser
        ser_time_all += ser_time

        (iter, subgraph, ser_time)
//        } else {
//          val subBis = new BufferedInputStream(new FileInputStream(sub_path))
//          val subbArray = Stream.continually(subBis.read).takeWhile(-1 !=).map(_.toByte).toArray
//          val subgraph = SparkConfiguration.deserialize[S](subbArray)
//          val (iter, ser) = sub2iter(computation, subgraph)
//          val ser_time = System.currentTimeMillis - ser
//          ser_time_all += ser_time
//          (iter, subgraph, ser_time)
//        }
      }

      private def sub2iter(computation: Computation[S], subgraph: S): (SubgraphEnumerator[S], Long) = {
        val ser = System.currentTimeMillis

        val iter = new KClistEnumerator[S]
        iter.set(computation, subgraph)
        iter.set(subgraph.getVertices)
        iter.rebuildState()
        val ser_time = System.currentTimeMillis - ser

        (iter, ser_time)
      }

      private def next_children(current: ComputationTree[S]): (ComputationTree[S], Boolean) = {
        var result = current
        var done = false

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

        (result, done)
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
