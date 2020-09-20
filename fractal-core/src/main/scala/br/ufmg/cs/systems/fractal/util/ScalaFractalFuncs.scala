package br.ufmg.cs.systems.fractal.util

import br.ufmg.cs.systems.fractal.computation._
import br.ufmg.cs.systems.fractal.graph._
import br.ufmg.cs.systems.fractal.subgraph._

import java.util.function.Predicate

/**
 * This is a set of aliases for extending fractal functions using the extended
 * syntax. The most typical use for this kind of pattern is when the function
 * requires a reusable local variable in order to avoid unnecessary object
 * creation.
 *
 */

trait SpecializedFunction3[
    @specialized(Int, Long, Double) -T1,
    @specialized(Int, Long, Double) -T2,
    @specialized(Int, Long, Double) -T3,
    @specialized(Int, Long, Double) +R] {
  def apply(t1: T1, t2: T2, t3: T3): R
}

trait VertexProcessFunc
    extends ((VertexInducedSubgraph, Computation[VertexInducedSubgraph]) => Unit) with Serializable

trait EdgeProcessFunc
    extends ((EdgeInducedSubgraph, Computation[EdgeInducedSubgraph]) => Unit)
    with Serializable

trait WordFilterFunc [S <: Subgraph] extends Serializable {
  def apply(t1: S, t2: Int, t3: Computation[S]): Boolean
}

trait ProcessComputeFunc [S <: Subgraph]
    extends ((SubgraphEnumerator[S], Computation[S]) => ComputationResults[S])
    with Serializable

trait MasterComputeFunc
    extends ((MasterComputation) => Unit)
    with Serializable

trait VertexFilterFunc [V] extends Predicate[Vertex[V]] with Serializable

trait EdgeFilterFunc [E] extends Predicate[Edge[E]] with Serializable
