package br.ufmg.cs.systems.fractal.graph;

import org.apache.commons.io.input.BOMInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
      long start = System.currentTimeMillis();
      try {
         BufferedReader reader = new BufferedReader(
               new InputStreamReader(new BOMInputStream(is)));

         String line = reader.readLine();
         System.out.println("readFromInputStream");
         int i = 0;
         while (line != null) {
            i++;
            if (i % 10000000 == 0) {
               System.out.println(i + " " + (System.currentTimeMillis() - start) / 1000.0 + "s");
            }

            StringTokenizer tokenizer = new StringTokenizer(line);

            int vertexId = Integer.parseInt(tokenizer.nextToken());

            while (tokenizer.hasMoreTokens()) {
               Edge edge = parseEdge(tokenizer, vertexId);
               addEdge(edge);
            }

            line = reader.readLine();
         }
         reader.close();
         //buildSortedNeighborhood();
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
