package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.util.collection.AtomicBitSetArray;
import br.ufmg.cs.systems.fractal.util.collection.ReclaimableIntCollection;
import com.koloboke.collect.IntCollection;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

public class LightBasicMainGraph<V,E> implements MainGraph<V,E> {
    private static final Logger LOG = Logger.getLogger(BasicMainGraph.class);

    public Map<Integer, List<Integer>> getAdjList() {
        return adjList;
    }

    private Map<Integer, List<Integer>> adjList = null;

    public int N = 0;
    // we keep a local (per JVM) pool of configurations potentially
    // representing several active fractal applications
    private static AtomicInteger nextGraphId = new AtomicInteger(0);
    protected int id = newGraphId();

    private static int newGraphId() {
        return nextGraphId.getAndIncrement();
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public void setId(int id) {
        this.id = id;
    }

    public void init() {
        adjList = new HashMap<>();
    }

    @Override
    public void reset() {}

    @Override
    public boolean isNeighborVertex(int v1, int v2) {
        return false;
    }

    @Override
    public MainGraph addVertex(Vertex vertex) {
        return null;
    }

    @Override
    public Vertex[] getVertices() {
        return new Vertex[0];
    }

    @Override
    public Vertex getVertex(int vertexId) {
        return null;
    }

    @Override
    public int getVertexId(int vertexIdx) {
        return 0;
    }

    @Override
    public int getNumberVertices() {
        return 0;
    }

    @Override
    public Edge[] getEdges() {
        return new Edge[0];
    }

    @Override
    public Edge getEdge(int edgeId) {
        return null;
    }

    @Override
    public int getNumberEdges() {
        return 0;
    }

    @Override
    public ReclaimableIntCollection getEdgeIds(int v1, int v2) {
        return null;
    }

    @Override
    public MainGraph addEdge(Edge e) {

        if(!adjList.containsKey(e.getSourceId())) {
            if (e.getSourceId() > N) N = e.getSourceId();
            adjList.put(e.getSourceId(), new ArrayList<>());
        }
        if(!adjList.containsKey(e.getDestinationId())) {
            if (e.getDestinationId() > N) N = e.getDestinationId();
            adjList.put(e.getDestinationId(), new ArrayList<>());
        }
        adjList.get(e.getSourceId()).add(e.getDestinationId());
        adjList.get(e.getDestinationId()).add(e.getSourceId());

        return null;
    }

    @Override
    public boolean areEdgesNeighbors(int edge1Id, int edge2Id) {
        return false;
    }

    @Override
    public boolean isNeighborEdge(int src1, int dest1, int edge2) {
        return false;
    }

    @Override
    public VertexNeighbourhood getVertexNeighbourhood(int vertexId) {
        //TODO
        return null;
    }

    @Override
    public IntCollection getVertexNeighbours(int vertexId) {
        return null;
    }

    @Override
    public boolean isEdgeLabelled() {
        return false;
    }

    @Override
    public boolean isMultiGraph() {
        return false;
    }

    @Override
    public void forEachEdgeId(int v1, int v2, IntConsumer intConsumer) {

    }

    //----------------------

    @Override
    public int filterVertices(AtomicBitSetArray tag) {
        return 0;
    }

    @Override
    public int filterVertices(Predicate<Vertex<V>> vpred) {
        return 0;
    }

    @Override
    public int filterEdges(AtomicBitSetArray tag) {
        return 0;
    }

    @Override
    public int filterEdges(Predicate<Edge<E>> epred) {
        return 0;
    }

    @Override
    public int undoVertexFilter() {
        return 0;
    }

    @Override
    public int undoEdgeFilter() {
        return 0;
    }

    @Override
    public int filter(AtomicBitSetArray vtag, AtomicBitSetArray etag) {
        return 0;
    }

    @Override
    public void buildSortedNeighborhood() {

    }
}