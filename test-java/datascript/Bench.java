package datascript;

import java.util.*;
import java.util.stream.*;
import clojure.lang.*;
import clojure.java.api.Clojure;
import datomic.btset.BTSet;

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
    int expected = 0;
    for (Object o: set) {
      assert ((Integer) o).intValue() == expected : "Expected " + expected + ", got " + o;
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

class RemoveBench extends ABench {
  ISortedSet set;
  List<Integer> removes;

  RemoveBench(Class<? extends ISortedSet> setClass, Collection<Integer> source, boolean asTransient, int maxLen) {
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

@SuppressWarnings("unchecked")
abstract class APersistentCollection<T> implements ISortedSet {
  T _impl;

  abstract APersistentCollection<T> create(T _impl);

  public ISortedSet with(Object o)    { return create((T) ((IPersistentCollection) _impl).cons(o)); }
  public ISortedSet without(Object o) { return create((T) ((IPersistentSet) _impl).disjoin(o)); }
  public int size()                   { return        ((IPersistentCollection) _impl).count(); }
  public boolean contains(Object o)   { return        ((IPersistentSet) _impl).contains(o); }

  public Iterator iterator()          { return        ((Iterable) _impl).iterator(); }
  public ISortedSet toTransient()     { return create((T) ((IEditableCollection) _impl).asTransient()); }
  public ISortedSet toPersistent()    { return create((T) ((ITransientCollection) _impl).persistent()); }
  public ISeq seq()                   { return        ((Seqable) _impl).seq(); }
}

class DatomicSet extends APersistentCollection<BTSet> {
  static IFn ctor;
  static {
    Symbol ns = (Symbol) Clojure.var("clojure.core", "symbol").invoke("datomic.api");
    Clojure.var("clojure.core", "require").invoke(ns);
    ctor = (IFn) Clojure.var("datomic.btset", "btset");
  }

  DatomicSet(BTSet impl) { _impl = impl; }
  DatomicSet(int ml) { _impl = (BTSet) ctor.invoke(); }
  DatomicSet create(BTSet impl) { return new DatomicSet(impl); }
}


class DatascriptSet extends APersistentCollection<SortedSet> {
  DatascriptSet(SortedSet impl) { _impl = impl; }
  DatascriptSet(int ml) {
    SortedSet.setMaxLen(ml);
    _impl = new SortedSet();
  }
  DatascriptSet create(SortedSet impl) { return new DatascriptSet(impl);}
}

class OldDatascriptSet extends APersistentCollection<Object> {
  static IFn ctor;
  static {
    Symbol ns = (Symbol) Clojure.var("clojure.core", "symbol").invoke("datascript.btset");
    Clojure.var("clojure.core", "require").invoke(ns);
    // ctor = (IFn) Clojure.var("datascript.btset", "btset-by");
    ctor = (IFn) Clojure.var("datascript.btset", "btset");
  }

  OldDatascriptSet(Object impl) { _impl = impl; }
  // OldDatascriptSet(int ml) { _impl = (Object) ctor.invoke(RT.DEFAULT_COMPARATOR); }
  OldDatascriptSet(int ml) { _impl = (Object) ctor.invoke(); }
  OldDatascriptSet create(Object impl) { return new OldDatascriptSet(impl); }
}


public class Bench {
  // static Integer[] maxLens = new Integer[]{8,16,32,64,128,256,512,1024};
  // static Integer[] maxLens = new Integer[]{32, 64, 128};
  static Integer[] maxLens = new Integer[]{64};
  static int warmups = 100;
  static int runs = 50;

  public static ISortedSet addAll(ISortedSet target, Collection source) {
    ISortedSet result = target.toTransient();
    for (Object o: source)
      result = result.with(o);
    return result.toPersistent();
  }

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

  public static void runBench(Class<? extends ABench> benchClass, Collection source, Class<? extends ISortedSet> setClass) throws Exception {
    runBench(benchClass, source, setClass, false);
  }

  public static void runBench(Class<? extends ABench> benchClass, Collection source, Class<? extends ISortedSet> setClass, boolean asTransient) throws Exception {
    System.out.print(String.format("%-20s", setClass.getSimpleName() + (asTransient ? "♻" : "")));
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

  public static void bench() throws Exception {
    new DatomicSet(0); // println warnings before any other output
    new OldDatascriptSet(0);

    System.out.println("Lengths             " + String.join("      ", Arrays.stream(maxLens).map((ml)-> ml.toString() + " max").collect(Collectors.toList())));

    ArrayList<Integer> source = randomList(100000);
    ArrayList<Integer> bigSource = randomList(1000000);

    System.out.println("\n                === 100K ADDs ===");
    runBench(AddBench.class, source, DatomicSet.class);
    // runBench(AddBench.class, source, OldDatascriptSet.class);
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
    runBench(AddBench.class, source, DatascriptSet.class,    false);
    runBench(AddBench.class, source, DatascriptSet.class,    true);
    
    System.out.println("\n                === 100K CONTAINS ===");
    runBench(ContainsBench.class, source, DatomicSet.class);
    // runBench(ContainsBench.class, source, OldDatascriptSet.class);
    // runBench(ContainsBench.class, source, SmallTransientSet.class );
    // runBench(ContainsBench.class, source, LinearSearchSet.class   );
    // runBench(ContainsBench.class, source, FlatIterSet.class       );
    // runBench(ContainsBench.class, source, ReverseFlatIterSet.class);
    // runBench(ContainsBench.class, source, AtomicBooleanSet.class);
    // runBench(ContainsBench.class, source, CleanupSet.class);
    runBench(ContainsBench.class, source, DatascriptSet.class);

    System.out.println("\n                === ITERATE♻ over 1M ===");
    // runBench(IterateBench.class, bigSource, OldDatascriptSet.class);
    // runBench(IterateBench.class, bigSource, LinearSearchSet.class   );
    // runBench(IterateBench.class, bigSource, FlatIterSet.class       );
    // runBench(IterateBench.class, bigSource, ReverseFlatIterSet.class);
    // runBench(IterateBench.class, bigSource, AtomicBooleanSet.class  );
    // runBench(IterateBench.class, bigSource, CleanupSet.class  );
    // runBench(IterateBench.class, bigSource, SliceSet.class  );
    runBench(IterateBench.class, bigSource, DatascriptSet.class);
 
    System.out.println("\n                === SEQ ITER over 1M ===");
    runBench(SeqIterateBench.class, bigSource, DatomicSet.class);
    // runBench(SeqIterateBench.class, bigSource, OldDatascriptSet.class);
    runBench(SeqIterateBench.class, bigSource, DatascriptSet.class);
 
    System.out.println("\n                === 100K REMOVEs ===");
    // runBench(RemoveBench.class, source, OldDatascriptSet.class);
    // runBench(RemoveBench.class, source, DisjoinSet.class,   false);
    // runBench(RemoveBench.class, source, DisjoinSet.class,   true);
    // runBench(RemoveBench.class, source, EarlyExitDisjSet.class, false);
    // runBench(RemoveBench.class, source, EarlyExitDisjSet.class, true);
    // runBench(RemoveBench.class, source, CleanupSet.class, false);
    // runBench(RemoveBench.class, source, CleanupSet.class, true);
    runBench(RemoveBench.class, source, DatascriptSet.class, false);
    runBench(RemoveBench.class, source, DatascriptSet.class, true);
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