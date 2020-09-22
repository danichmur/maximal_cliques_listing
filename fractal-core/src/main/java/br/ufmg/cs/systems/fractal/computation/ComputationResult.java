package br.ufmg.cs.systems.fractal.computation;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

public class ComputationResult<E extends Subgraph> {
    SubgraphEnumerator<E> enumerator;
    E subgraph;

    ComputationResult(SubgraphEnumerator<E> enumerator, E subgraph) {
        this.enumerator = enumerator;
        this.subgraph = subgraph;
    }

}