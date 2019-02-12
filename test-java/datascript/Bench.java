package datascript;

import java.util.*;
import java.util.stream.*;
import clojure.lang.*;
import clojure.java.api.Clojure;
import datomic.btset.BTSet;

abstract class ABench {
  IPersistentSet empty;
  Collection source;
  boolean useTransient;
  
  ABench(IPersistentSet empty, Collection source, boolean useTransient) {
    this.empty = empty;
    this.source = source;
    this.useTransient = useTransient;
  }

  IPersistentSet newSet(Collection source) {
    if (useTransient) { 
      ITransientCollection set = ((IEditableCollection) empty).asTransient();
      for (Object o: source)
        set = set.conj(o);
      return (IPersistentSet) set.persistent();
    } else {
      IPersistentSet set = empty;
      for (Object o: source)
        set = (IPersistentSet) set.cons(o);
      return set;
    }
  }

  abstract void run();
}


class AddBench extends ABench {
  AddBench(IPersistentSet empty, Collection source, boolean useTransient) {
    super(empty, source, useTransient);
  }

  void run() {
    newSet(source);
  }
}


class ContainsBench extends ABench {
  IPersistentSet _set;

  ContainsBench(IPersistentSet empty, Collection source, boolean useTransient) {
    super(empty, source, useTransient);
    _set = newSet(source);
  }

  void run() {
    for (Object o: source)
      if (false == _set.contains(o))
        throw new RuntimeException("Must contain " + o);
  }
}


class IterateBench extends ABench {
  Iterable set;

  IterateBench(IPersistentSet empty, Collection source, boolean useTransient) {
    super(empty, source, useTransient);
    set = (Iterable) newSet(source);
  }

  void run() {
    int expected = 0;
    for (Object o: set) {
      assert ((Integer) o).intValue() == expected : "Expected " + expected + ", got " + o;
      ++expected;
    }
    assert expected == source.size() : "Expected " + source.size() + ", got " + expected;
  }
}


class SeqIterateBench extends ABench {
  Seqable set;

  SeqIterateBench(IPersistentSet empty, Collection source, boolean useTransient) {
    super(empty, source, useTransient);
    set = newSet(source);
  }

  void run() {
    int expected = 0;
    ISeq seq = set.seq();
    while (seq != null) {
      Integer value = (Integer) seq.first();
      assert value.intValue() == expected : "Expected " + expected + ", got " + value;
      ++expected;
      seq = seq.next();
    }
    assert expected == source.size() : "Expected " + source.size() + ", got " + expected;
  }
}


class BoundSliceBench extends ABench {
  ISortedSet set;

  BoundSliceBench(IPersistentSet empty, Collection source, boolean useTransient) {
    super(empty, source, useTransient);
    set = (ISortedSet) newSet(source);
  }

  void run() {
    int from = 1, to = 999999, expected = from;
    ISeq seq = set.slice(from, to);
    Integer value = -1;
    while (seq != null) {
      value = (Integer) seq.first();
      assert value.intValue() == expected : "Expected " + expected + ", got " + value;
      ++expected;
      seq = seq.next();
    }
    assert value.intValue() == to : "Expected " + to + ", got " + value;
  }
}


class RemoveBench extends ABench {
  IPersistentSet set;
  List<Integer> removes;

  RemoveBench(IPersistentSet empty, Collection<Integer> source, boolean useTransient) {
    super(empty, source, useTransient);
    set = newSet(source);
    removes = new ArrayList<>(source);
    Collections.shuffle(removes);
  }

  void run() {
    IPersistentSet s = set;

    if (useTransient) {
      ITransientSet ts = (ITransientSet) ((IEditableCollection) s).asTransient();
      for (Object o: removes)
        ts = ts.disjoin(o);
      s = (IPersistentSet) ts.persistent();
    } else {
      for (Object o: removes)
        s = s.disjoin(o);
    }

    if (s.count() != 0)
      throw new RuntimeException("count " + s.count());
  }
}


public class Bench {
  // static Integer[] maxLens = new Integer[]{8,16,32,64,128,256,512,1024};
  // static Integer[] maxLens = new Integer[]{8, 64, 1024};
  // static Integer[] maxLens = new Integer[]{32, 64, 128};
  static Integer[] maxLens = new Integer[]{64};
  static int warmups = 100;
  static int runs = 50;

  public static ArrayList<Integer> randomList(int size) {
    ArrayList<Integer> res = new ArrayList<>();
    IntStream.range(0, size).forEach((int l) -> res.add(l));
    Collections.shuffle(res);
    return res;
  }

  public static String range(Collection<Long> coll) {
    long min = coll.stream().reduce(Long.MAX_VALUE, (a, b) -> a<b?a:b),
         max = coll.stream().reduce(0L, (a,b) -> a<b?b:a);
    return String.format("%2d..%2dms", min, max);
  }

  public static void runBench(Class<? extends ABench> benchClass, Collection source, IPersistentSet empty) throws Exception {
    runBench(benchClass, source, empty, false);
  }

  public static void runBench(Class<? extends ABench> benchClass, Collection source, IPersistentSet empty, boolean useTransient) throws Exception {
    System.out.print(String.format("%-20s", empty.getClass().getSimpleName() + (useTransient ? "♻" : "")));
    boolean isSortedSet = empty.getClass().equals(SortedSet.class);

    for(int maxLen: isSortedSet ? maxLens : new Integer[]{0}) {
      if (isSortedSet)
        SortedSet.setMaxLen(maxLen);

      ABench bench = benchClass.getDeclaredConstructor(IPersistentSet.class, Collection.class, boolean.class).newInstance(empty, source, useTransient);
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

  public static void bench() throws Exception {
    // println warnings before any other output
    Symbol ns = (Symbol) Clojure.var("clojure.core", "symbol").invoke("datomic.api");
    Clojure.var("clojure.core", "require").invoke(ns);
    BTSet datomicSet = (BTSet) ((IFn) Clojure.var("datomic.btset", "btset")).invoke();

    PersistentTreeSet ptSet = PersistentTreeSet.EMPTY;
    SortedSet datascriptSet = SortedSet.EMPTY;

    System.out.println("Lengths             " + String.join("      ", Arrays.stream(maxLens).map((ml)-> ml.toString() + " max").collect(Collectors.toList())));

    ArrayList<Integer> source = randomList(100000);
    ArrayList<Integer> bigSource = randomList(1000000);

    System.out.println("\n                === 100K ADDs ===");
    runBench(AddBench.class, source, ptSet);
    runBench(AddBench.class, source, datomicSet);
    runBench(AddBench.class, source, datascriptSet);
    runBench(AddBench.class, source, datascriptSet, true);
    
    System.out.println("\n                === 100K CONTAINS ===");
    runBench(ContainsBench.class, source, ptSet);
    runBench(ContainsBench.class, source, datomicSet);
    runBench(ContainsBench.class, source, datascriptSet);

    System.out.println("\n                === ITERATE♻ over 1M ===");
    runBench(IterateBench.class, bigSource, ptSet);
    runBench(IterateBench.class, bigSource, datascriptSet);
 
    System.out.println("\n                === SEQ ITER over 1M ===");
    runBench(SeqIterateBench.class, bigSource, ptSet);
    runBench(SeqIterateBench.class, bigSource, datomicSet);
    runBench(SeqIterateBench.class, bigSource, datascriptSet);

    System.out.println("\n                === Bound slice over 1M ===");
    runBench(BoundSliceBench.class, bigSource, ClojureSet.EMPTY);
    runBench(BoundSliceBench.class, bigSource, datascriptSet);
 
    System.out.println("\n                === 100K REMOVEs ===");
    runBench(RemoveBench.class, source, ptSet);
    runBench(RemoveBench.class, source, datascriptSet);
    runBench(RemoveBench.class, source, datascriptSet, true);
  }

  public static void test() throws Exception {
    SortedSet.setMaxLen(4);
    SortedSet s = (SortedSet) new SortedSet().asTransient();

    for(Integer l: randomList(100))
      s = s.cons(l);

    s = (SortedSet) s.persistent();

    // for(Integer l: randomList(20))
    //   s = s.disjoin(l);

    Comparator<Integer> cmp = (a, b) -> (a / 10) - (b / 10);

    System.out.println("30.5 .. 30.5:    " + s.slice(30.5, 30.5));

    System.out.println("\nForward");

    System.out.println("30   .. 39:      " + s.slice(30, 39));
    System.out.println("30   .. 39:      " + s.rslice(39, 30).rseq());

    System.out.println("30.5 .. 39.5:    " + s.slice(30.5, 39.5));
    System.out.println("30.5 .. 39.5:    " + s.rslice(39.5, 30.5).rseq());

    System.out.println("null .. 9:       " + s.slice(null, 9));
    System.out.println("null .. 9:       " + s.rslice(9, null).rseq());

    System.out.println("90   .. null:    " + s.slice(90, null));
    System.out.println("90   .. null:    " + s.rslice(null, 90).rseq());


    System.out.println("\nBackwards");

    System.out.println("39   .. 30:      " + s.rslice(39, 30));
    System.out.println("39   .. 30:      " + s.slice(30, 39).rseq());

    System.out.println("39.5 .. 30.5:    " + s.rslice(39.5, 30.5));
    System.out.println("39.5 .. 30.5:    " + s.slice(30.5, 39.5).rseq());

    System.out.println("9    .. null:    " + s.rslice(9, null));
    System.out.println("9    .. null:    " + s.slice(null, 9).rseq());

    System.out.println("null .. 90:      " + s.rslice(null, 90));
    System.out.println("null .. 90:      " + s.slice(90, null).rseq());


    System.out.println("\nCustom comparator");

    System.out.println("30   .. 30 % 10: " + s.slice(30, 30, cmp));
    System.out.println("30   .. 30 % 10: " + s.rslice(30, 30, cmp).rseq());

    System.out.println("30   .. 40 % 10: " + s.slice(30, 40, cmp));
    System.out.println("30   .. 40 % 10: " + s.rslice(40, 30, cmp).rseq());

    System.out.println("40   .. 30 % 10: " + s.rslice(40, 30, cmp));
    System.out.println("40   .. 30 % 10: " + s.slice(30, 40, cmp).rseq());
   
    System.out.println("\nReduces");
    // System.out.println(s.slice(null, null).reduce(new AFn() { public Object invoke(Object x, Object y) { return ((Integer)x)+((Integer)y); }}));
    IFn plus = (IFn) Clojure.var("clojure.core", "+");
    System.out.println("reduced sum(0 .. 100) 4950 = " + s.slice(null, null).reduce(plus));
    System.out.println("reduced sum(100 .. 0) 4950 = " + s.rslice(null, null).reduce(plus));
    System.out.println("reduced sum(10 .. 20) 165  = " + s.slice(10, 20).reduce(plus));
    System.out.println("reduced sum(20 .. 10) 165  = " + s.rslice(20, 10).reduce(plus));

    IFn reduce1 = (IFn) Clojure.var("clojure.core", "reduce1");
    System.out.println("chunked sum(0 .. 100) 4950 = " + reduce1.invoke(plus, 0, s.slice(null, null)));
    System.out.println("chunked sum(100 .. 0) 4950 = " + reduce1.invoke(plus, 0, s.rslice(null, null)));
    System.out.println("chunked sum(10 .. 20) 165  = " + reduce1.invoke(plus, 0, s.slice(10, 20)));
    System.out.println("chunked sum(20 .. 10) 165  = " + reduce1.invoke(plus, 0, s.rslice(20, 10)));
  }

  public static void main(String args[]) throws Exception {
    bench();
    test();
  }
}