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

    public  static FrozenDataHolder current;

    public  static synchronized void addFrozenData(FrozenDataHolder pFrozenData){
        frozenList.add(pFrozenData);
    }

    public static Set<FrozenDataHolder> getFrozenList() {
        return frozenList;
    }

    public static void cleanFrozenList(List<Set<Integer>> cliques) {
        Set<FrozenDataHolder> filteredFrozenList = new HashSet<>();
        for (FrozenDataHolder h : frozenList) {
            boolean isOk = isHolderOk(cliques, h);
            if (isOk) {
                filteredFrozenList.add(h);
            }
        }

        frozenList = filteredFrozenList;
        System.out.println();
    }

    private static boolean isHolderOk(List<Set<Integer>> cliques, FrozenDataHolder holder) {
        for (Set<Integer> clique : cliques) {
            if (isPrefixInClique(clique, holder.freezePrefix)) {
                return false;
            }
            if (holder.freezePrefix.containsAny(clique)) {
                dagRemoveAll(clique, holder.freezeDag);
            }
            // && isDagContainsAny(clique, holder.freezeDag)

//            int prefixSize = holder.freezePrefix.size();
//            holder.freezePrefix.removeAll(clique);
//            int filteredPrefixSize = holder.freezePrefix.size();
//
//            if (filteredPrefixSize != prefixSize) {
//                dagRemoveAll(clique, holder.freezeDag);
//            }
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

    private static void dagRemoveAll(Set<Integer> clique, IntObjMap<IntArrayList> dag) {
        for (int c : clique) {
            dag.remove(c);
        }
    }

    private static boolean isPrefixInClique(Set<Integer> clique, IntArrayList prefix) {
        List<Integer> cList = new ArrayList<>(clique);
        for (int i = 0; i < cList.size() - 1; i++) {
            for (int j = i + 1; j < cList.size(); j++) {
                if (prefix.contains(cList.get(i)) && prefix.contains(cList.get(j))) {
                    return true;
                }
            }
        }
        return false;
    }
}
