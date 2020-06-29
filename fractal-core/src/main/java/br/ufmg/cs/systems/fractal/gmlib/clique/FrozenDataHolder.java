package br.ufmg.cs.systems.fractal.gmlib.clique;

import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.map.IntObjCursor;
import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.collect.set.IntSet;

import java.util.*;

public class FrozenDataHolder {


    public  IntObjMap<IntArrayList> freezeDag;

    public  IntArrayList freezePrefix;

    public FrozenDataHolder(IntObjMap<IntArrayList> freezeDag, IntArrayList freezePrefix) {
        this.freezeDag = HashIntObjMaps.newMutableMap(freezeDag.size());
        IntObjCursor<IntArrayList> cur = freezeDag.cursor();
        while (cur.moveNext()) {
            this.freezeDag.put(cur.key(), new IntArrayList(cur.value()));
        }
        this.freezePrefix = new IntArrayList(freezePrefix.toIntArray());

    }

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
    @Override
    public int hashCode() {
        return Objects.hash(freezeDag, freezePrefix);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        FrozenDataHolder holder = (FrozenDataHolder) obj;

        if (holder.freezePrefix.equalsCollection(freezePrefix)) {
            IntSet holderDag = holder.freezeDag.keySet();
            IntSet dag = freezeDag.keySet();
            if (holderDag.size() != dag.size()) {
                return false;
            }
            if (holderDag.size() == 0) {
                return true;
            }
            return holderDag.containsAll(dag);
        }

        return false;
    }
}
