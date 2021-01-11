package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

import java.util.ArrayList;
import java.util.List;

public class ComputationResults<S extends Subgraph> {
    private List<ComputationResult<S>> results;

    public boolean isEmpty() {
        return results.isEmpty();
    }

    public int size() {
        return results.size();
    }

    public ComputationResults() {
        results = new ArrayList<>();
    }

    public ComputationResult<S> get(int i) {
        return results.get(i);
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

    public void add(S subgraph) {
        results.add(new ComputationResult<>(subgraph));
    }

    public void add(String serializedFileIter, String serializedFileSub) {
        results.add(new ComputationResult<>(serializedFileIter, serializedFileSub));
    }

    public void add(int vertex) {
        results.add(new ComputationResult<>(vertex));
    }

    public void addAll(ComputationResults<S> newResults) {
        results.addAll(newResults.results);
    }

    public void removeFirst() {
        results.remove(0);
    }
}