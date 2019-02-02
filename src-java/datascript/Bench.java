package datascript;

import java.util.*;
import java.util.stream.*;
import clojure.lang.*;

abstract class ABench {
  Class<? extends ISortedSet> setClass;
  Collection source;
  boolean asTransient;
  int maxLen;

  
  ABench(Class<? extends ISortedSet> setClass, Collection source, boolean asTransient, int maxLen) {
    this.setClass = setClass;
    this.source = source;
    this.asTransient = asTransient;
    this.maxLen = maxLen;
  }

  ISortedSet newSet() {
    try {
      return setClass.getDeclaredConstructor(int.class).newInstance(maxLen);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  ISortedSet newSet(Collection source) {
    ISortedSet set = newSet();
    if (asTransient)
      set = set.toTransient();
    for (Object o: source)
      set = set.with(o);
    return asTransient ? set.toPersistent() : set;
  }

  abstract void run();
}


class AddBench extends ABench {
  AddBench(Class<? extends ISortedSet> setClass, Collection source, boolean asTransient, int maxLen) {
    super(setClass, source, asTransient, maxLen);
  }

  void run() {
    newSet(source);
  }
}


class ContainsBench extends ABench {
  ISortedSet set;

  ContainsBench(Class<? extends ISortedSet> setClass, Collection source, boolean asTransient, int maxLen) {
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
  ISortedSet set;

  IterateBench(Class<? extends ISortedSet> setClass, Collection source, boolean asTransient, int maxLen) {
    super(setClass, source, asTransient, maxLen);
    set = newSet(source);
  }

  void run() {
    long expected = 0;
    for (Object o: set) {
      assert ((Long) o).longValue() == expected : "Expected " + expected + ", got " + o;
      ++expected;
    }
    assert expected == source.size() : "Expected " + source.size() + ", got " + expected;
  }
}

class SeqIterateBench extends ABench {
  ISortedSet set;

  SeqIterateBench(Class<? extends ISortedSet> setClass, Collection source, boolean asTransient, int maxLen) {
    super(setClass, source, asTransient, maxLen);
    set = newSet(source);
  }

  void run() {
    long expected = 0;
    ISeq seq = set.seq();
    while (seq != null) {
      Long value = (Long) seq.first();
      assert value.longValue() == expected : "Expected " + expected + ", got " + value;
      ++expected;
      seq = seq.next();
    }
    assert expected == source.size() : "Expected " + source.size() + ", got " + expected;
  }
}

class RemoveBench extends ABench {
  ISortedSet set;
  List<Long> removes;

  RemoveBench(Class<? extends ISortedSet> setClass, Collection<Long> source, boolean asTransient, int maxLen) {
    super(setClass, source, asTransient, maxLen);
    set = newSet(source);
    removes = new ArrayList<>(source);
    Collections.shuffle(removes);
  }

  void run() {
    ISortedSet s = set;
    if (asTransient)
      s = s.toTransient();
    for (Object o: removes)
      s = s.without(o);
    if (asTransient)
      s = s.toPersistent();
    if (s.size() != 0)
      throw new RuntimeException("size " + s.size());
  }
}

public class Bench {
  static Integer[] maxLens = new Integer[]{8,16,32,64,128,256,512,1024};
  // static Integer[] maxLens = new Integer[]{32, 64, 128};
  // static Integer[] maxLens = new Integer[]{64};
  static int warmups = 100;
  static int runs = 50;

  public static ISortedSet addAll(ISortedSet target, Collection source) {
    ISortedSet result = target.toTransient();
    for (Object o: source)
      result = result.with(o);
    return result.toPersistent();
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

  public static void runBench(Class<? extends ABench> benchClass, Collection source, Class<? extends ISortedSet> setClass) throws Exception {
    runBench(benchClass, source, setClass, true);
  }

  public static void runBench(Class<? extends ABench> benchClass, Collection source, Class<? extends ISortedSet> setClass, boolean asTransient) throws Exception {
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
    // runBench(AddBench.class, source, CleanupSet.class,    false);
    // runBench(AddBench.class, source, CleanupSet.class,    true);
    // runBench(AddBench.class, source, ClojureSet.class,    false);
    runBench(AddBench.class, source, ClojureSet.class,    true);
    
    System.out.println("\n                === 100K CONTAINS ===");
    // runBench(ContainsBench.class, source, SmallTransientSet.class );
    // runBench(ContainsBench.class, source, LinearSearchSet.class   );
    // runBench(ContainsBench.class, source, FlatIterSet.class       );
    // runBench(ContainsBench.class, source, ReverseFlatIterSet.class);
    // runBench(ContainsBench.class, source, AtomicBooleanSet.class);
    // runBench(ContainsBench.class, source, CleanupSet.class);
    runBench(ContainsBench.class, source, ClojureSet.class);

    System.out.println("\n                === ITERATE over 1M ===");
    // runBench(IterateBench.class, bigSource, LinearSearchSet.class   );
    // runBench(IterateBench.class, bigSource, FlatIterSet.class       );
    // runBench(IterateBench.class, bigSource, ReverseFlatIterSet.class);
    // runBench(IterateBench.class, bigSource, AtomicBooleanSet.class  );
    // runBench(IterateBench.class, bigSource, CleanupSet.class  );
    // runBench(IterateBench.class, bigSource, SliceSet.class  );
    runBench(IterateBench.class, bigSource, ClojureSet.class);
 
    System.out.println("\n                === SEQ ITER over 1M ===");
    runBench(SeqIterateBench.class, bigSource, ClojureSet.class);
 
    System.out.println("\n                === 100K REMOVEs ===");
    // runBench(RemoveBench.class, source, DisjoinSet.class,   false);
    // runBench(RemoveBench.class, source, DisjoinSet.class,   true);
    // runBench(RemoveBench.class, source, EarlyExitDisjSet.class, false);
    // runBench(RemoveBench.class, source, EarlyExitDisjSet.class, true);
    // runBench(RemoveBench.class, source, CleanupSet.class, false);
    // runBench(RemoveBench.class, source, CleanupSet.class, true);
    // runBench(RemoveBench.class, source, ClojureSet.class, false);
    runBench(RemoveBench.class, source, ClojureSet.class, true);

    ClojureSet.setMaxLen(4);
    ClojureSet s = (ClojureSet) new ClojureSet().toTransient();

    for(Long l: randomList(20))
      s = s.with(l);

    // s = (ClojureSet) s.toPersistent();

    // for(Long l: randomList(20))
    //   s = s.without(l);

    System.out.println(s);
    System.out.println(s.str());

    // Comparator<Integer> cmp = (a, b) -> (a / 10) - (b / 10);
    // ISeq seq = s.slice(30, 30, cmp);
    // while (seq != null) {
    //   System.out.println(seq.first() + " -- " + seq.toString());
    //   // if (!((ClojureSet.Seq) seq).mutableNext()) break;
    //   seq = seq.next();
    // }

    // System.out.println(s.slice(30, 30, cmp).reduce(new AFn() { public Object invoke(Object x, Object y) { return ((Integer)x)+((Integer)y); }}));
  }
}