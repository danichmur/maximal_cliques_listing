package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

import java.util.ArrayList;
import java.util.List;

public class ComputationResults<S extends Subgraph> {
    private List<ComputationResult<S>> results;

    public boolean isEmpty() {
        return results.isEmpty();
    }

    public ComputationResults() {
        results = new ArrayList<>();
    }

    public List<ComputationResult<S>> getResults() {
        return results;
    }

    public int getStep() {
        if (results.size() > 0) {
            return results.get(0).subgraph.getVertices().size();
        } else {
            return 0;
        }
    }

    public void add(SubgraphEnumerator<S> enumerator, S subgraph) {
        results.add(new ComputationResult<>(enumerator, subgraph));
    }

    public void addAll(ComputationResults<S> newResults) {
        results.addAll(newResults.results);
    }
}