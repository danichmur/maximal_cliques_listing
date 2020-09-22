package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

import java.util.List;

public class ComputationResultsTree<S extends Subgraph> {
    private List<ComputationResultsTree<S>> sons;
    private ComputationResult<S> value;

    public ComputationResultsTree(ComputationResult<S> value,  List<ComputationResultsTree<S>> sons) {
        this.value = value;
        this.sons = sons;
    }

    public ComputationResultsTree(ComputationResult<S> value) {
        this.value = value;
        this.sons = null;
    }

    public List<ComputationResultsTree<S>> getSons() {
        return sons;
    }

    public ComputationResult<S> getValue() {
        return value;
    }

    public void setSons(List<ComputationResultsTree<S>> sons) {
        this.sons = sons;
    }
}
