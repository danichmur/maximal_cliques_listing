package br.ufmg.cs.systems.fractal.gmlib.clique;

import br.ufmg.cs.systems.fractal.util.collection.IntArrayList;
import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class GlobalFreezeHolder {

    public static boolean freeze = false;
    public static FrozenDataHolder current;

    private static final Object lock = new Object();
    private static String path = "";
    private static final String FROZEN_NAME = "frozen_lists";

    private static Map<Integer /*clique size*/, Integer /*current file*/> filenames = new HashMap<>();
    private static TreeSet<Integer> availableSizes = new TreeSet<>();

    public static void setPath(String pPath) {
        path = pPath;
    }

    public static void addFrozenData(FrozenDataHolder pFrozenData) {
        if (pFrozenData.getSize() <= 2) {
            //get rid of single edges
            return;
        }

        synchronized(lock) {
            saveToFile(pFrozenData);
        }
    }

    public static boolean isFrozenAvailable() {
        boolean isAvailable;

        synchronized(lock) {
            isAvailable = availableSizes.size() != 0;
        }

        return isAvailable;
    }

//    public static void cleanFrozenList(List<Set<Integer>> cliques) {
//        if (cliques.size() == 0) {
//            return;
//        }
//        Set<FrozenDataHolder> filteredFrozenList = new HashSet<>();
//        for (FrozenDataHolder h : frozenList) {
//            boolean isOk = isHolderOk(cliques, h);
//            if (isOk) {
//                filteredFrozenList.add(h);
//            }
//        }
//
//        frozenList = filteredFrozenList;
//    }

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
            if (holder.getSize() <= 2) {
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

    private static void saveToFile(FrozenDataHolder pFrozenData) {
        int size = pFrozenData.freezePrefix.size() + pFrozenData.freezeDag.size();
        int filename = filenames.getOrDefault(size, 0);

        availableSizes.add(size);
        filenames.put(size, filename + 1);
        Path p = getPath(size, filename);

        File f = new File(String.valueOf(p));
        f.getParentFile().mkdirs();

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(f))) {
            pFrozenData.saveToFile(oos);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static FrozenDataHolder pollFirstAvailable(int cliqueSize, List<Set<Integer>> cliques) {
        synchronized(lock) {
            int availableSize = availableSizes.last();
            if (availableSize >= cliqueSize) {
                int filename = filenames.getOrDefault(availableSize, -1);

                while (filename > 0) {
                    filename--;
                    String p = String.valueOf(getPath(availableSize, filename));
                    File f = new File(String.valueOf(p));
                    filenames.put(availableSize, filename);
                    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f))) {
                        FrozenDataHolder holder = FrozenDataHolder.readFile(ois);
                        if (cliques.size() == 0 || isHolderOk(cliques, holder)) {
                            if (holder.getSize() >= cliqueSize) {
                                return holder;
                            } else {
                                //sizes of holder were changed, so we should to resave holder
                                saveToFile(holder);
                            }
                        } else {
                            f.delete();
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
                //folder is empty, we are done here
                availableSizes.remove(availableSize);
            }
        }
        return null;
    }

    private static Path getPath(int size, int num) {
        return Paths.get(path, FROZEN_NAME, String.valueOf(size), String.valueOf(num));
    }

    public static void deleteFrozenDir() {
        try {
            FileUtils.deleteDirectory(new File(String.valueOf(Paths.get(path, FROZEN_NAME))));
        } catch (IOException ignored) {}
    }
}