package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.aggregation.AggregationStorage;
import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.pattern.Pattern;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.Pair;
import com.koloboke.collect.IntCollection;
import org.apache.hadoop.io.Writable;

import java.util.List;

public interface Computation<S extends Subgraph> {

    // {{{ initialization
    void init(Configuration<S> config);
    void initAggregations(Configuration<S> config);
    ComputationResults<S> compute(S Subgraph);
    Computation<S> nextComputation();
    void finish();
    // }}}

    // {{{ runtime
    SubgraphEnumerator<S> expandCompute(S Subgraph);
    IntCollection getPossibleExtensions(S Subgraph);
    ComputationResults<S> processCompute(SubgraphEnumerator<S> expansions);
    boolean filter(S Subgraph);
    void process(S Subgraph);
    boolean filter(S existingSubgraph, int newWord);
    // }}}

    // {{{ Output
    void output(S Subgraph);
    // }}}

    // {{{ Aggregation-related stuff
    <K extends Writable, V extends Writable>
       AggregationStorage<K, V> readAggregation(String name);
    
    <K extends Writable, V extends Writable> 
       AggregationStorage<K, V> getAggregationStorage(String name);

    <K extends Writable, V extends Writable>
       void map(String name, K key, V value);
    // }}}

    // {{{ Misc
    int getStep();

    int getPartitionId();

    int getNumberPartitions();

    Configuration<S> getConfig();

    boolean shouldBypass();
    // }}}

    // {{{ Internal
    void setExecutionEngine(CommonExecutionEngine<S> executionEngine);
    CommonExecutionEngine<S> getExecutionEngine();
    
    String computationLabel();
    int setDepth(int depth);
    int getDepth();

    SubgraphEnumerator<S> getSubgraphEnumerator();
    SubgraphEnumerator<S> forkEnumerator(Computation<S> computation);

    Class<? extends Subgraph> getSubgraphClass();
    
    int getInitialNumWords();

    boolean containsWord(int wordId);

    Pattern getPattern();
    // }}}
}
