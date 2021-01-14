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

    public void setResultType(ResultType resultType) {
        this.resultType = resultType;
    }

    public ResultType getResultType() {
        return resultType;
    }

    private ResultType resultType;

    ComputationResult(SubgraphEnumerator<S> enumerator, S subgraph) {
        //System.out.println("ResultType.REGULAR");
        resultType = ResultType.REGULAR;
        this.enumerator = enumerator;
        this.subgraph = subgraph;
    }

    ComputationResult(S subgraph) {
        //System.out.println("ResultType.SUBGRAPH");
        resultType = ResultType.SUBGRAPH;
        this.subgraph = subgraph;
    }

    ComputationResult(String serializedFileIter, String serializedFileSub) {
        //System.out.println("ResultType.SERIALIZED");
        resultType = ResultType.SERIALIZED;
        this.serializedFileIter = serializedFileIter;
        this.serializedFileSub = serializedFileSub;
    }

    ComputationResult(int v, boolean first) {
        if (first) {
            //System.out.println("ResultType.VERTEX");
            resultType = ResultType.VERTEX;
            vertex = v;
        } else {
            //System.out.println("ResultType.SUBGRAPH ONE");
            resultType = ResultType.SUBGRAPH;
            vertex = v;
        }
    }

    void reset() {
        subgraph = null;
        enumerator = null;
        serializedFileIter = "";
        serializedFileSub = "";
    }

}