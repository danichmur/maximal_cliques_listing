package br.ufmg.cs.systems.fractal.computation;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

public class ComputationResult<S extends Subgraph> {
    SubgraphEnumerator<S> enumerator;
    S subgraph;

    ComputationResult(SubgraphEnumerator<S> enumerator, S subgraph) {
        this.enumerator = enumerator;
        this.subgraph = subgraph;
    }

}