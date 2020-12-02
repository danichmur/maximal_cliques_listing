package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.subgraph.Subgraph;

import java.util.List;
import java.util.ArrayList;

public class ComputationTree<S extends Subgraph> {
    Computation<S> nextComputation;
    ComputationResult<S> head;
    ComputationTree<S> parent;
    private List<ComputationTree<S>> children;
    private int firstNotVisited = 0;
    public int level;
    private static int idCounter = 0;
    public int id;

    ComputationTree(ComputationTree<S> parent, Computation<S> nextComputation, ComputationResult<S> head) {
        this.nextComputation = nextComputation;
        this.head = head;
        this.children = new ArrayList<>();
        this.parent = parent;
        if (parent == null) {
            this.level = 0;
        } else {
            this.level = parent.level + 1;
        }

        id = idCounter++;
    }

    ComputationTree(Computation<S> nextComputation, ComputationResult<S> head) {
        this(null, nextComputation, head);
    }

    public ComputationTree<S> visit() {
        if (hasUnvisited()) {
            ComputationTree<S> child = children.get(firstNotVisited);
            //children.set(firstNotVisited, null);
            firstNotVisited++;
            return child;
        } else {
            return null;
        }
    }

    public boolean hasUnvisited() {
        return firstNotVisited < this.children.size();
    }

    public void adopt(ComputationTree<S> orphan) {
        children.add(orphan);
    }

    public boolean hasParent() {
        return parent != null;
    }

    public void killChildren() {
        children = new ArrayList<>();
    }
}
