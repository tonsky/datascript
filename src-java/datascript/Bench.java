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
  int maxLen;
  
  ABench(Class<? extends IPersistentSet> setClass, Collection source, int maxLen) {
    this.setClass = setClass;
    this.source = source;
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
    IPersistentSet set = newSet().asTransient();
    for (Object o: source)
      set = set.add(o);
    return set.persistent();
  }

  abstract void run();
}


class AddBench extends ABench {
  AddBench(Class<? extends IPersistentSet> setClass, Collection source, int maxLen) {
    super(setClass, source, maxLen);
  }

  void run() {
    newSet(source);
  }
}


class ContainsBench extends ABench {
  IPersistentSet set;

  ContainsBench(Class<? extends IPersistentSet> setClass, Collection source, int maxLen) {
    super(setClass, source, maxLen);
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

  IterateBench(Class<? extends IPersistentSet> setClass, Collection source, int maxLen) {
    super(setClass, source, maxLen);
    set = newSet(source);
  }

  void run() {
    long expected = 0;
    for (Object o: set)
      if (((Long) o).longValue() != expected++)
        throw new RuntimeException("Expected " + expected + ", got " + o);
  }
}

public class Bench {
  // static Integer[] maxLens = new Integer[]{8,16,32,64,128,256,512,1024};
  static Integer[] maxLens = new Integer[]{32, 64, 128};
  static int warmups = 20;
  static int runs = 40;

  public static IPersistentSet addAll(IPersistentSet target, Collection source) {
    IPersistentSet result = target.asTransient();
    for (Object o: source)
      result = result.add(o);
    return result.persistent();
  }

  public static ArrayList<Long> randomList(int size) {
    ArrayList<Long> res = new ArrayList<Long>();
    LongStream.range(0, size).forEach((long l) -> res.add(l));
    Collections.shuffle(res);
    return res;
  }

  public static String range(Collection<Long> coll) {
    long min = coll.stream().reduce(Long.MAX_VALUE, (a, b) -> a<b?a:b),
         max = coll.stream().reduce(0L, (a,b) -> a<b?b:a);
    return String.format("%2d..%2dms", min, max);
  }

  public static void runBench(Class<? extends IPersistentSet> setClass, Collection source, Class<? extends ABench> benchClass) throws Exception {
    System.out.print(String.format("%-20s", setClass.getSimpleName()));
    for(int maxLen: maxLens) {
      ABench bench = benchClass.getDeclaredConstructor(Class.class, Collection.class, int.class).newInstance(setClass, source, maxLen);
      for(int i=0; i < warmups; ++i)
        bench.run();

      ArrayList<Long> measurments = new ArrayList<Long>();
      for(int i=0; i < runs; ++i) {
        long t0 = System.currentTimeMillis();
        bench.run();
        measurments.add(Long.valueOf(System.currentTimeMillis() - t0));
      }
      System.out.print(String.format("%-12s", range(measurments)));
    }
    System.out.println();
  }

  public static void main(String args[]) throws Exception {
    System.out.println("Lengths             " + String.join("      ", Arrays.stream(maxLens).map((ml)-> ml.toString() + " max").collect(Collectors.toList())));

    ArrayList<Long> source = randomList(100000);
    ArrayList<Long> bigSource = randomList(1000000);

    System.out.println("\n                === 100K ADDs ===");
    runBench(PersistentBTSet.class,    source, AddBench.class);
    // runBench(TransientBTSet.class,     source, AddBench.class);
    // runBench(TwoNodesSet.class,        source, AddBench.class);
    // runBench(ExtraLeafSet.class,       source, AddBench.class);
    // runBench(EarlyExitSet.class,       source, AddBench.class);
    runBench(SmallTransientSet.class,  source, AddBench.class);
    runBench(LinearSearchSet.class,    source, AddBench.class);
    // runBench(FlatIterSet.class,        source, AddBench.class);
    // runBench(ReverseFlatIterSet.class, source, AddBench.class);

    
    System.out.println("\n                === 100K CONTAINS ===");
    runBench(SmallTransientSet.class,  source, ContainsBench.class);
    runBench(LinearSearchSet.class,    source, ContainsBench.class);
    // runBench(FlatIterSet.class,        source, ContainsBench.class);
    // runBench(ReverseFlatIterSet.class, source, ContainsBench.class);

    System.out.println("\n                === ITERATE over 1M ===");
    runBench(LinearSearchSet.class,    bigSource, IterateBench.class);    
    runBench(FlatIterSet.class,        bigSource, IterateBench.class);
    runBench(ReverseFlatIterSet.class, bigSource, IterateBench.class);

    // IPersistentSet s = addAll(new ReverseFlatIterSet(4), randomList(20));
    // System.out.println(s);
    // for(Object l : s)
    //   System.out.println(l);
  }
}