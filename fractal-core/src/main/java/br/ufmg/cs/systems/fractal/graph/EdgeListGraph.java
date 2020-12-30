package br.ufmg.cs.systems.fractal.graph;

import br.ufmg.cs.systems.fractal.util.collection.IntSet;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.apache.commons.io.input.BOMInputStream;

import java.io.*;
import java.nio.file.Path;
import java.util.StringTokenizer;

public class EdgeListGraph<V,E> extends BasicMainGraph<V,E> {
   
   public EdgeListGraph(String name) {
      super(name, false, false);
   }

   public EdgeListGraph(String name, boolean isEdgeLabelled,
         boolean isMultiGraph) {
      super(name, isEdgeLabelled, isMultiGraph);
   }

   public EdgeListGraph(Path filePath, boolean isEdgeLabelled,
         boolean isMultiGraph) throws IOException {
      super(filePath.getFileName().toString(), isEdgeLabelled, isMultiGraph);
   }

   public EdgeListGraph(org.apache.hadoop.fs.Path hdfsPath,
         boolean isEdgeLabelled, boolean isMultiGraph) throws IOException {
      super(hdfsPath.getName(), isEdgeLabelled, isMultiGraph);
   }

   @Override
   protected void readFromInputStream(InputStream is) {
      try {
         IntSet v = new IntSet();
         for (int i = 0; i < 10000; i++) {
            v.add(i);
         }

         mainGraph = ChronicleMapBuilder
                 .of(Integer.class, IntSet.class)
                 .name("main-graph")
                 .entries(1_000_0000)
                 .averageValue(v)
                 //.maxChunksPerEntry(1_000_000)
                 //.actualChunkSize(100_000)
                 //.actualSegments(1)
                 .createPersistedTo(new File("map.dat"));
      } catch (IOException e) {
         e.printStackTrace();
      }

      long start = System.currentTimeMillis();
      try {
         BufferedReader reader = new BufferedReader(
               new InputStreamReader(new BOMInputStream(is)));

         String line = reader.readLine();
         System.out.println("readFromInputStream");
         int i = 0;
         int source = -1;
         IntSet vertexNeighbourhood = new IntSet();

         while (line != null) {
            i++;
            if (i % 10_000_000 == 0) {
               System.out.println(i + " " + (System.currentTimeMillis() - start) / 1000.0 + "s");
            }

            StringTokenizer tokenizer = new StringTokenizer(line);

            int vertexId = Integer.parseInt(tokenizer.nextToken());

            while (tokenizer.hasMoreTokens()) {
               Edge edge = parseEdge(tokenizer, vertexId);
               //addEdge(edge);

               numEdges++;
               if (numVertices < edge.getDestinationId()) numVertices = edge.getDestinationId();
               if (numVertices < edge.getSourceId()) numVertices = edge.getSourceId();

               if (source != vertexId) {
                  if (source != -1) {
                     mainGraph.put(source, vertexNeighbourhood);

                     vertexNeighbourhood = mainGraph.get(vertexId);
                     if (vertexNeighbourhood == null) {
                        vertexNeighbourhood = new IntSet();
                     }
                  }
                  source = vertexId;
               }
               vertexNeighbourhood.add(edge.getDestinationId());

               ensureCanStoreNewVertices(numVertices);
               VertexNeighbourhood vertexNeighbourhood1 = vertexNeighborhoods[edge.getDestinationId()];
               if (vertexNeighbourhood1 == null) {
                  vertexNeighbourhood1 = createVertexNeighbourhood();
                  vertexNeighborhoods[edge.getDestinationId()] = vertexNeighbourhood1;
               }

               vertexNeighbourhood1.addEdge(edge.getSourceId(), edge.getEdgeId());
            }


            line = reader.readLine();
         }
         //the last one
         mainGraph.put(source, vertexNeighbourhood);
         reader.close();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }


   @Override
   protected Edge parseEdge(StringTokenizer tokenizer, int vertexId) {
      int neighborId = Integer.parseInt(tokenizer.nextToken());
      return createEdge(vertexId, neighborId);
   }
}
