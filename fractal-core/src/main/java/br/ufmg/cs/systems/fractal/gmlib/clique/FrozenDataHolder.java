package br.ufmg.cs.systems.fractal.gmlib.clique;

import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.set.IntSet;

import java.util.Arrays;
import java.util.Objects;

public class FrozenDataHolder {


    public  IntObjMap<IntArrayList> freezeDag;

    public  IntArrayList freezePrefix;

    public FrozenDataHolder(IntObjMap<IntArrayList> freezeDag, IntArrayList freezePrefix) {
        this.freezeDag = freezeDag;
        this.freezePrefix = freezePrefix;
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
        if (Arrays.equals(holder.freezePrefix.getBackingArray(), freezePrefix.getBackingArray())) {
            IntSet holderDag = holder.freezeDag.keySet();
            IntSet dag = freezeDag.keySet();
            if (holderDag.size() != dag.size()) {
                return false;
            }
            return holderDag.containsAll(dag);
        }

        return false;
    }
}
