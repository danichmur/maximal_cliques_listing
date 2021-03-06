package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.collection.IntSet;
import br.ufmg.cs.systems.fractal.util.collection.ReclaimableIntCollection;
import com.koloboke.collect.IntCollection;

import java.util.List;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

public interface MainGraph<V,E> {
    int getId();
    
    void setId(int id);

    void reset();

    boolean isNeighborVertex(int v1, int v2);

    MainGraph addVertex(Vertex vertex);

    Vertex[] getVertices();

    Vertex getVertex(int vertexId);

    int getVertexId(int vertexIdx);

    int getNumberVertices();

    Edge[] getEdges();

    Edge getEdge(int edgeId);

    int getNumberEdges();

    ReclaimableIntCollection getEdgeIds(int v1, int v2);

    MainGraph addEdge(Edge edge);

    boolean areEdgesNeighbors(int edge1Id, int edge2Id);

    @Deprecated
    boolean isNeighborEdge(int src1, int dest1, int edge2);

    VertexNeighbourhood getVertexNeighbourhood(int vertexId);

    IntArrayList getVertexNeighbours(int vertexId);
    IntSet getReversedVertexNeighbours(int vertexId);
    void cleanReversedNeighbourhood();

    boolean isEdgeLabelled();

    boolean isMultiGraph();

    void forEachEdgeId(int v1, int v2, IntConsumer intConsumer);

    int filterVertices(AtomicBitSetArray tag);

    int filterVertices(Predicate<Vertex<V>> vpred);

    int filterEdges(AtomicBitSetArray tag);

    int filterEdges(Predicate<Edge<E>> epred);

    int undoVertexFilter();
    
    int undoEdgeFilter();

    int filter(AtomicBitSetArray vtag, AtomicBitSetArray etag);

    void buildSortedNeighborhood();

    void removeLowers(int i);

    void closeMap();

    void removeCliques(List<IntArrayList> cliques);

}
