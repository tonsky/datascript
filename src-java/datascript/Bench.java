package datascript;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.LongStream;
import java.util.stream.Collectors;

abstract class ABench {
  Class<? extends IPersistentSet> setClass;
  Collection source;
  boolean asTransient;
  int maxLen;

  
  ABench(Class<? extends IPersistentSet> setClass, Collection source, boolean asTransient, int maxLen) {
    this.setClass = setClass;
    this.source = source;
    this.asTransient = asTransient;
    this.maxLen = maxLen;
  }

  IPersistentSet newSet() {
    try {
      return setClass.getDeclaredConstructor(int.class).newInstance(maxLen);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  IPersistentSet newSet(Collection source) {
    IPersistentSet set = newSet();
    if (asTransient)
      set = set.asTransient();
    for (Object o: source)
      set = set.add(o);
    return asTransient ? set.persistent() : set;
  }

  abstract void run();
}


class AddBench extends ABench {
  AddBench(Class<? extends IPersistentSet> setClass, Collection source, boolean asTransient, int maxLen) {
    super(setClass, source, asTransient, maxLen);
  }

  void run() {
    newSet(source);
  }
}


class ContainsBench extends ABench {
  IPersistentSet set;

  ContainsBench(Class<? extends IPersistentSet> setClass, Collection source, boolean asTransient, int maxLen) {
    super(setClass, source, asTransient, maxLen);
    set = newSet(source);
  }

  void run() {
    for (Object o: source)
      if (false == set.contains(o))
        throw new RuntimeException("Must contain " + o);
  }
}

class IterateBench extends ABench {
  IPersistentSet set;

  IterateBench(Class<? extends IPersistentSet> setClass, Collection source, boolean asTransient, int maxLen) {
    super(setClass, source, asTransient, maxLen);
    set = newSet(source);
  }

  void run() {
    long expected = 0;
    for (Object o: set)
      if (((Long) o).longValue() != expected++)
        throw new RuntimeException("Expected " + expected + ", got " + o);
  }
}

class RemoveBench extends ABench {
  IPersistentSet set;
  List removes;

  RemoveBench(Class<? extends IPersistentSet> setClass, Collection source, boolean asTransient, int maxLen) {
    super(setClass, source, asTransient, maxLen);
    set = newSet(source);
    removes = new ArrayList(source);
    Collections.shuffle(removes);
  }

  void run() {
    IPersistentSet s = set;
    if (asTransient)
      s = s.asTransient();
    for (Object o: removes)
      s = s.remove(o);
    if (s.size() != 0 || s.depth() != 1)
      throw new RuntimeException("size " + s.size() + "depth " + s.depth());
  }
}

public class Bench {
  // static Integer[] maxLens = new Integer[]{8,16,32,64,128,256,512,1024};
  // static Integer[] maxLens = new Integer[]{32, 64, 128};
  static Integer[] maxLens = new Integer[]{64};
  static int warmups = 60;
  static int runs = 40;

  public static IPersistentSet addAll(IPersistentSet target, Collection source) {
    IPersistentSet result = target.asTransient();
    for (Object o: source)
      result = result.add(o);
    return result.persistent();
  }

  public static ArrayList<Long> randomList(int size) {
    ArrayList<Long> res = new ArrayList<>();
    LongStream.range(0, size).forEach((long l) -> res.add(l));
    Collections.shuffle(res);
    return res;
  }

  public static String range(Collection<Long> coll) {
    long min = coll.stream().reduce(Long.MAX_VALUE, (a, b) -> a<b?a:b),
         max = coll.stream().reduce(0L, (a,b) -> a<b?b:a);
    return String.format("%2d..%2dms", min, max);
  }

  public static void runBench(Class<? extends ABench> benchClass, Collection source, Class<? extends IPersistentSet> setClass) throws Exception {
    runBench(benchClass, source, setClass, true);
  }

  public static void runBench(Class<? extends ABench> benchClass, Collection source, Class<? extends IPersistentSet> setClass, boolean asTransient) throws Exception {
    System.out.print(String.format("%-20s", setClass.getSimpleName() + (asTransient ? "" : "🔒")));
    for(int maxLen: maxLens) {
      ABench bench = benchClass.getDeclaredConstructor(Class.class, Collection.class, boolean.class, int.class).newInstance(setClass, source, asTransient, maxLen);
      for(int i=0; i < warmups; ++i)
        bench.run();

      ArrayList<Long> measurements = new ArrayList<>();
      for(int i=0; i < runs; ++i) {
        long t0 = System.currentTimeMillis();
        bench.run();
        measurements.add(Long.valueOf(System.currentTimeMillis() - t0));
      }
      System.out.print(String.format("%-12s", range(measurements)));
    }
    System.out.println();
  }

  public static void main(String args[]) throws Exception {
    System.out.println("Lengths             " + String.join("      ", Arrays.stream(maxLens).map((ml)-> ml.toString() + " max").collect(Collectors.toList())));

    ArrayList<Long> source = randomList(100000);
    ArrayList<Long> bigSource = randomList(1000000);

    System.out.println("\n                === 100K ADDs ===");
    // runBench(AddBench.class, source, PersistentBTSet.class,    false);
    // runBench(AddBench.class, source, TransientBTSet.class,     true);
    // runBench(AddBench.class, source, TwoNodesSet.class,        true);
    // runBench(AddBench.class, source, ExtraLeafSet.class,       true);
    // runBench(AddBench.class, source, EarlyExitSet.class,       true);
    // runBench(AddBench.class, source, SmallTransientSet.class,  true);
    // runBench(AddBench.class, source, LinearSearchSet.class,    true);
    // runBench(AddBench.class, source, FlatIterSet.class,        true);
    // runBench(AddBench.class, source, ReverseFlatIterSet.class, true);
    // runBench(AddBench.class, source, AtomicBooleanSet.class,   false);
    // runBench(AddBench.class, source, AtomicBooleanSet.class,   true);
    
    System.out.println("\n                === 100K CONTAINS ===");
    // runBench(ContainsBench.class, source, SmallTransientSet.class );
    // runBench(ContainsBench.class, source, LinearSearchSet.class   );
    // runBench(ContainsBench.class, source, FlatIterSet.class       );
    // runBench(ContainsBench.class, source, ReverseFlatIterSet.class);
    // runBench(ContainsBench.class, source, AtomicBooleanSet.class  );

    System.out.println("\n                === ITERATE over 1M ===");
    // runBench(IterateBench.class, bigSource, LinearSearchSet.class   );
    // runBench(IterateBench.class, bigSource, FlatIterSet.class       );
    // runBench(IterateBench.class, bigSource, ReverseFlatIterSet.class);
    // runBench(IterateBench.class, bigSource, AtomicBooleanSet.class  );

    System.out.println("\n                === 100K REMOVEs ===");
    runBench(RemoveBench.class, source, DisjoinSet.class,   false);
    runBench(RemoveBench.class, source, DisjoinSet.class,   true);
    runBench(RemoveBench.class, source, EarlyExitDisjSet.class, false);
    runBench(RemoveBench.class, source, EarlyExitDisjSet.class, true);

    // DisjoinSet.setMaxLen(4);
    // DisjoinSet s = new DisjoinSet();
    // s = (DisjoinSet) addAll(s, randomList(20));
    // s = s.asTransient();
    // System.out.println(s);
    // for(Long l: randomList(20)) {
    //   System.out.println("Removing " + l);
    //   s = s.remove(l);
    //   System.out.println(s.str());
    // }
  }
}