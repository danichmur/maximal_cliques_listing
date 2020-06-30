package br.ufmg.cs.systems.fractal.gmlib.clique;

import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.map.IntObjMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GlobalFreezeHolder {

    public static boolean freeze = false;

    private static Set<FrozenDataHolder> frozenList = new HashSet<>();
    public static FrozenDataHolder current;
    private static final Object lock = new Object();

    public static void addFrozenData(FrozenDataHolder pFrozenData) {
        synchronized(lock) {
            frozenList.add(pFrozenData);
        }
    }

    public static Set<FrozenDataHolder> getFrozenList() {
        return frozenList;
    }

    public static void unfreeze(FrozenDataHolder h) {
        frozenList.remove(h);
    }

    public static boolean isFrozenAvailable() {
        boolean isAvailable;
        synchronized(lock) {
            isAvailable = frozenList.size() != 0;
        }
        return isAvailable;
    }

    public static void cleanFrozenList(List<Set<Integer>> cliques) {
        if (cliques.size() == 0) {
            return;
        }
        Set<FrozenDataHolder> filteredFrozenList = new HashSet<>();
        for (FrozenDataHolder h : frozenList) {
            boolean isOk = isHolderOk(cliques, h);
            if (isOk) {
                filteredFrozenList.add(h);
            }
        }

        frozenList = filteredFrozenList;
    }

    private static boolean isHolderOk(List<Set<Integer>> cliques, FrozenDataHolder holder) {
        for (Set<Integer> clique : cliques) {
            if (holder.isPrefixInClique(clique)) {
                return false;
            }
            if (holder.freezePrefix.containsAny(clique)) {
                holder.clearDag(clique);
            }
            // && isDagContainsAny(clique, holder.freezeDag)
//            int prefixSize = holder.freezePrefix.size();
//            holder.freezePrefix.removeAll(clique);
//            int filteredPrefixSize = holder.freezePrefix.size();
//
            if (holder.freezePrefix.size() + holder.freezeDag.size() <= 2) {
                //get rid of single edges
                return false;
            }

        }
        return true;
    }

    private static boolean isDagContainsAny(Set<Integer> clique, IntObjMap<IntArrayList> dag) {
        if (dag.size() == 0) {
            //base case
            return false;
        }
        for (int c : clique) {
            if (dag.containsKey(c)) {
                return true;
            }
        }
        return false;
    }
}