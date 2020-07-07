# Composite tuples

A tuple is a collection of scalar values, represented in memory as a Clojure vector.

Composite tuples are applicable:

- when a domain entity has a multi-attribute key;
- to optimize a query that joins more than one high-population attributes on the same entity.

Composite attributes are entirely managed by DataScript–you never assert or retract them yourself. Whenever you assert or retract any attribute that is part of a composite, DataScript will automatically populate the composite value. If the current value of an entity does not include all attributes of a composite, the missing attributes will be nil.

Some ground rules:

- tuple attribute must be of cardinality one;
- tuple attribute can’t reference cardinality many attributes;
- tuple attribute can’t reference other tuple attributes;
- tuple attributes are indexed by default.

For example, consider the domain with attributes :a, :b, :c and a composite tuple `:a+b+c`

```
{:a+b+c {:db/tupleAttrs [:a :b :c]}}
```

If we transact something like this:

```
[{:db/id 1, :a "a", :b "b"}]
```

you might notice that entity 1 got composite attribute `:a+b+c` populated automatically:

```
(d/pull db '[*] 1)
; => {:db/id 1, :a "a", :b "b", :a+b+c ["a" "b" nil]}
```

If you change attributes, tuple value is updated automatically:

```
(d/transact! conn
  [{:db/id 1, :a "A", :b "B", :c "c"}])
; => {:db/id 1, :a "A", :b "B", :c "c", :a+b+c ["A" "B" "c"]}
```

If you remove ALL attributes that make up a tuple, tuple attribute is retracted too:

```
(d/transact! conn
  [[:db/add 1 :d "d"]
   [:db/retract 1 :a]
   [:db/retract 1 :b]
   [:db/retract 1 :c]])
; => {:db/id 1, :d "d"}
```

Direct update of tuple attributes is not allowed:

```
(d/transact! conn
  [{:db/id 1, :a+b+c ["A" "B" "c"]}])
; => clojure.lang.ExceptionInfo: Can’t modify tuple attrs directly: [:db/add 1 :a+b+c ["A" "B" "c"]]
```

Tuple attributes are automatically indexed:

```
(d/index-range db :a+b+c ["A" "B" "C"] ["a" "b" "c"])
; => [#db/Datom 1 :a+b+c ["a" "b" "c"]]
```

If you mark tuple attribute as `:db/unique :db.unique/value`, you get an uniqueness by composite key.

```
(def conn (d/create-conn {:a+b {:db/tupleAttrs [:a :b]
                                :db/unique :db.unique/value}}))

(d/transact! conn
  [{:db/id 1, :a "A", :b "B"}
   {:db/id 2, :a "A", :b "b"}
   {:db/id 3, :a "a", :b "B"}
   {:db/id 4, :a "a", :b "b"}])
```

Neither `:a` nor `:b` are unique per se, but their combination together is, enforced by DataScript:

```
(d/transact! conn
  [{:db/id 5, :a "A", :b "B"}])
; => clojure.lang.ExceptionInfo: Cannot add #datascript/Datom [5 :a+b ["A" "B"] 536870916 true] because of unique constraint: (#datascript/Datom [1 :a+b ["A" "b"] 536870915 true])
```

If you mark tuple attribute as `:db/unique :db.unique/identity`, you might use it in lookup refs:

```
(def conn (d/create-conn {:a+b {:db/tupleAttrs [:a :b]
                                :db/unique :db.unique/identity}}))

(d/entity (d/db conn) [:a+b ["a" "b"]])
; => {:db/id 4, :a "a", :b "b", :a+b ["a" "b"]}
```

Composite tuples are like normal attributes in most cases. Feel free to use them in lookup refs, upserts, queries, index access.