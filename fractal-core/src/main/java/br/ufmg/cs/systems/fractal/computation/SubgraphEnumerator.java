package br.ufmg.cs.systems.fractal.computation;

import br.ufmg.cs.systems.fractal.conf.Configuration;
import br.ufmg.cs.systems.fractal.subgraph.Subgraph;
import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import br.ufmg.cs.systems.fractal.util.pool.IntArrayListPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import com.koloboke.collect.map.IntObjMap;
import org.apache.log4j.Logger;

public class SubgraphEnumerator<S extends Subgraph> implements Iterator<S> {
   protected static final Logger LOG = Logger.getLogger(SubgraphEnumerator.class);

   protected ReentrantLock rlock;

   protected Computation<S> computation;

   protected IntArrayList prefix;

   protected S subgraph;

   protected boolean lastHasNext;

   protected int currElem = -1;

   protected IntCollection wordIds;

   protected IntCursor cur;

   public boolean shouldRemoveLastWord;
   public boolean extend = true;

   private AtomicBoolean active;
   private boolean frozen;

   public boolean isGetFirstCandidate() {
      return getFirstCandidate;
   }

   public void setGetFirstCandidate(boolean getFirstCandidate) {
      this.getFirstCandidate = getFirstCandidate;
   }

   private boolean getFirstCandidate;

   public String computationLabel() {
      return computation.computationLabel();
   }

   public boolean isActive() {
      return active != null && active.get();
   }


   public SubgraphEnumerator() {
      this.rlock = new ReentrantLock();
      this.prefix = IntArrayListPool.instance().createObject();
   }

   public void setForFrozen(S subgraph, IntObjMap<IntArrayList>  dag) {
      //subgraph.setVertices(prefix);
      this.subgraph = subgraph;
      set(dag.keySet());

      if(wordIds.size() == 1){
         cur.moveNext();
         currElem = cur.elem();
      }

   }

   public void setForFrozen(IntObjMap<IntArrayList> dag) {
      set(dag.keySet());

      if(wordIds.size() == 1){
         cur.moveNext();
         currElem = cur.elem();
      }

   }

   public IntObjMap<IntArrayList> getDag(){
      return null;
   }

   /**
    * Enumerator initialization. We assume a default no-parameter constructor
    * for the subgraph enumerator and use this method to initialize any internal
    * structures the custom implementation may need.
    * @param config current configuration
    */
   public void init(Configuration<S> config) {
      // empty by default
   }

   /**
    * Called after a internal/external work-stealing to reconstruct this
    * enumerator state for an alternative execution thread.
    */
   public void rebuildState() {
      // empty by default
   }

   public void clearDag() {

   }


   /**
    * This method is used to generate the set of extensions in preparation for
    * extension routines.
    */
   public void computeExtensions() {
      IntCollection extensions = subgraph.computeExtensions(computation);
      set(extensions);
   }

   public int getAdditionalSize() {
      return 0;
   }

   /**
    * An extend call consumes an extension and returns the next enumerator,
    * equivalent to the current one plus the extension. The default
    * implementation is memory efficient because it reuses the same structure
    * in-place for further extensions (returns 'this').
    * @return the updated extended subgraph enumerator
    */
   public SubgraphEnumerator<S> extend(int u) {
      next();
      return this;
   }

   public synchronized SubgraphEnumerator<S> set(
           Computation<S> computation, S subgraph) {
      this.computation = computation;
      this.subgraph = subgraph;
      return this;
   }


   public synchronized SubgraphEnumerator<S> set(IntCollection wordIds) {
      this.prefix.clear();

      this.prefix.addAll(subgraph.getWords());
      this.lastHasNext = false;
      int[] arr = wordIds.toIntArray();
      Arrays.sort(arr);

      this.wordIds = new IntArrayList(arr);

      this.cur = this.wordIds.cursor();

      this.shouldRemoveLastWord = false;
      this.active = new AtomicBoolean(true);
      return this;
   }

   public synchronized SubgraphEnumerator<S> forkEnumerator(Computation<S> computation) {
      System.out.println("forkEnumerator");
      // create new consumer, adding just enough to verify if there is still
      // work in it
      SubgraphEnumerator<S> iter = computation.
              getConfig().createSubgraphEnumerator(computation.shouldBypass());
      iter.subgraph = computation.getConfig().createSubgraph();
      iter.rlock = this.rlock;
      iter.computation = computation;
      iter.lastHasNext = false;
      iter.cur = this.cur;
      iter.wordIds = this.wordIds;
      iter.shouldRemoveLastWord = false;
      iter.active = this.active;
      iter.frozen = this.frozen;

      // expensive operations, only do if iterator is not empty
      if (iter.hasNext()) {
         iter.prefix.clear();
         iter.prefix.addAll(this.prefix);

         if (prefix.size() > 0) {
            iter.subgraph.addWord(prefix.getUnchecked(0));
         }

         for (int i = 1; i < prefix.size(); ++i) {
            iter.subgraph.nextExtensionLevel(subgraph);
            iter.subgraph.addWord(prefix.getUnchecked(i));
         }

         iter.rebuildState();
      }

      return iter;
   }

   public synchronized void joinConsumer() {
      //IntArrayListPool.instance().reclaimObject(prefix);
   }

   public void maybeRemoveLastWord() {
      if (shouldRemoveLastWord) {
         subgraph.removeLastWord();
         shouldRemoveLastWord = false;
      }
   }

   @Override
   public boolean hasNext() {
      // if currElem has a valid word to be consumed
      if (lastHasNext) {
         return true;
      }

      // this test is to make sure we do not remove the last word in the
      // first *hasNext* call
      maybeRemoveLastWord();

      try {
         rlock.lock();
         if (isActive()) {
            // skip extensions that turn the subgraph not canonical
            while (cur.moveNext()) {
               currElem = cur.elem();
               if (computation.filter(subgraph, currElem)) {
                  lastHasNext = true;
                  return true;
               }
            }
            //TODO
            //active.set(false);
         } else {
            maybeRemoveLastWord();
         }
         return false;
      } finally {
         rlock.unlock();
      }
   }

   @Override
   public S next() {
      shouldRemoveLastWord = true;
      subgraph.addWord(nextElem());
      return subgraph;
   }

   public int nextElem() {
      lastHasNext = false;
      return currElem;
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException();
   }

   public Computation<S> getComputation() {
      return this.computation;
   }

   public IntArrayList getPrefix() {
      return prefix;
   }

   public S getSubgraph() {
      return subgraph;
   }

   public IntCollection getWordIds() {
      return wordIds;
   }

   public void resetCursor() {
      this.cur = wordIds.cursor();
      this.currElem = -1;
      shouldRemoveLastWord = true;
      maybeRemoveLastWord();
   }

   @Override
   public String toString() {
      return "SubgraphEnumerator(" +
         "active=" + active +
              ",cur=" + cur +
              ",wordIds=" + wordIds +
              ",shouldRemoveLastWord=" + shouldRemoveLastWord +
              ",subgraph=" + subgraph +
         ",prefix=" + prefix + ")";
   }
}

