//TODO DO NOT USE NOW
package br.ufmg.cs.systems.fractal.gmlib.clique;

import com.twitter.cassovary.graph.node.SynchronizedDynamicNode;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FrozenDataHolderOld implements Serializable {

    public  List<SynchronizedDynamicNode> freezeDag;

    public Seq freezePrefix;

    public FrozenDataHolderOld() {

    }

//    public FrozenDataHolderOld(IntObjMap<IntArrayList> freezeDag, IntArrayList freezePrefix) {
//        this.freezePrefix = SynchronizedNodeBuilder.arrayList2Seq(freezePrefix);
//        IntObjCursor<IntArrayList> cur = freezeDag.cursor();
//
//        this.freezeDag = new ArrayList<>();
//
//        while (cur.moveNext()) {
//            this.freezeDag.add(SynchronizedNodeBuilder.build(cur.key(), cur.value()));
//        }
//    }

    void clearDag(Set<Integer> clique) {
        for (int c : clique) {
            freezeDag.remove(c);
        }
    }

    boolean isPrefixInClique(Set<Integer> clique) {
        List<Integer> cList = new ArrayList<>(clique);
        for (int i = 0; i < cList.size() - 1; i++) {
            for (int j = i + 1; j < cList.size(); j++) {
                if (freezePrefix.contains(cList.get(i)) && freezePrefix.contains(cList.get(j))) {
                    return true;
                }
            }
        }
        return false;
    }

//    void saveToFile(ObjectOutputStream oos) throws IOException {
//        oos.writeObject(freezePrefix);
//        oos.writeObject(freezeDag.size());
//        IntObjCursor<IntArrayList> cur = freezeDag.cursor();
//        while (cur.moveNext()) {
//            oos.writeObject(cur.key());
//            oos.writeObject(cur.value());
//        }
//    }

//    public static FrozenDataHolder readFile(ObjectInputStream ois) throws IOException, ClassNotFoundException {
//        IntArrayList prefix = (IntArrayList) ois.readObject();
//        Integer dagSize = (Integer) ois.readObject();
//        IntObjMap<IntArrayList> dag = HashIntObjMaps.newMutableMap(dagSize);
//
//        for (int i = 0; i < dagSize; i++) {
//            dag.put((int) ois.readObject(), (IntArrayList) ois.readObject());
//        }
//        return new FrozenDataHolder(dag, prefix);
//    }

//    @Override
//    public int hashCode() {
//        return Objects.hash(freezeDag, freezePrefix);
//    }
//
//    @Override
//    public boolean equals(Object obj) {
//        if (obj == this) {
//            return true;
//        }
//
//        if (obj == null || obj.getClass() != this.getClass()) {
//            return false;
//        }
//        FrozenDataHolder holder = (FrozenDataHolder) obj;
//
//        if (holder.freezePrefix.equalsCollection(freezePrefix)) {
//            IntSet holderDag = holder.freezeDag.keySet();
//            IntSet dag = freezeDag.keySet();
//            if (holderDag.size() != dag.size()) {
//                return false;
//            }
//            if (holderDag.size() == 0) {
//                return true;
//            }
//            return holderDag.containsAll(dag);
//        }
//
//        return false;
//    }

    public int getSize() {
        return freezeDag.size() + freezePrefix.size();
    }
}
