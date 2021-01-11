package br.ufmg.cs.systems.fractal.computation;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

enum ResultType {
    VERTEX,
    SUBGRAPH,
    SERIALIZED,
    REGULAR
}

public class ComputationResult<S extends Subgraph> {
    SubgraphEnumerator<S> enumerator;
    S subgraph;
    String serializedFileIter = "";
    String serializedFileSub = "";
    int vertex = -1;

    public ResultType getResultType() {
        return resultType;
    }

    private ResultType resultType;

    ComputationResult(SubgraphEnumerator<S> enumerator, S subgraph) {
        resultType = ResultType.REGULAR;
        this.enumerator = enumerator;
        this.subgraph = subgraph;
    }

    ComputationResult(S subgraph) {
        resultType = ResultType.SUBGRAPH;
        this.subgraph = subgraph;
    }

    ComputationResult(String serializedFileIter, String serializedFileSub) {
        resultType = ResultType.SERIALIZED;
        this.serializedFileIter = serializedFileIter;
        this.serializedFileSub = serializedFileSub;
    }

    ComputationResult(int v) {
        resultType = ResultType.VERTEX;
        vertex = v;
    }

    void reset() {
        subgraph = null;
        enumerator = null;
        serializedFileIter = "";
        serializedFileSub = "";
    }

}