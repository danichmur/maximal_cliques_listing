package br.ufmg.cs.systems.fractal.computation;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

public class ComputationResult<S extends Subgraph> {
    SubgraphEnumerator<S> enumerator;
    S subgraph;
    String serializedFileIter = "";
    String serializedFileSub = "";
    int vertex = -1;

    ComputationResult(SubgraphEnumerator<S> enumerator, S subgraph) {
        this.enumerator = enumerator;
        this.subgraph = subgraph;
    }

    ComputationResult(String serializedFileIter, String serializedFileSub) {
        this.serializedFileIter = serializedFileIter;
        this.serializedFileSub = serializedFileSub;
    }

    ComputationResult(int v) {
        vertex = v;
    }

}