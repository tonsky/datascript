# DatomicScript

> What if creating Datomic database would be as cheap as creating a Hashmap?

An implementation of Datomic in-memory database and Datalog query engine in ClojureScript.

DatomicScript is meant to run inside browser. It is cheap to create, quick to query and ephemeral. You create a database on page load, put some data in it, track changes, do queries and forget about it when user closes the page.

DatomicScript databases are immutable and based on persistent data structures. In fact, they’re more like a data structures (think Hashmap). Unlike querying real SQL DB, when you query DatomicScript, it all comes down to a Hashmap lookup. Or series of lookups. Or array iteration. There’s no particular overhead to it. You put little data in it, it’s fast. You put a lot of data, well, at least it has indexes. That should do better than you filtering an array by hand anyway. The thing is really lightweight.

DatomicScript intention is to be a basic building block in client-side applications that needs to track a lot of state during their lifetime. There’s a lot of benefits:

- Central, uniform approach to manage all application state. Clients to work with state become decoupled and independent: rendering, server sync, undo/redo do not interfere with each other.
- Immutability simplifies things even in single-threaded browser environment. Keep track of app state evolution, rewind to any point in time, always render consistent state, sync in background without locking anybody.
- Datalog query engine to answer non-trivial questions about current app state.
- Structured format to track data coming in and out of DB. Datalog queries can be run against it too.

Also check out blog post about [how DatomicScript fits into current webdev ecosystem](http://tonsky.me/blog/decomposing-web-app-development/).

## Usage examples [![Build Status](https://travis-ci.org/tonsky/datomicscript.svg?branch=master)](https://travis-ci.org/tonsky/datomicscript)

```clj
:dependencies [
  [org.clojure/clojurescript "0.0-2173"]
  ...
  [datomicscript "0.1.3"]
]
```

```clj
(require '[datomicscript :as d])

;; Implicit join, multi-valued attribute

(let [schema {:aka {:cardinality :many}}
      conn   (d/create-conn schema)]
  (d/transact! conn [ { :db/id -1
                        :name  "Maksim"
                        :age   45
                        :aka   ["Maks Otto von Stirlitz", "Jack Ryan"] } ])
  (d/q '[ :find  ?n ?a
          :where [?e :aka "Maks Otto von Stirlitz"]
                 [?e :name ?n]
                 [?e :age  ?a] ]
       @conn))

;; => #{ ["Maksim" 45] }


;; Desctucturing, function call, predicate call, query over collection

(d/q '[ :find  ?k ?x
        :in    [[?k [?min ?max]] ...] ?range
        :where [(?range ?min ?max) [?x ...]]
               [(even? ?x)] ]
      { :a [1 7], :b [2 4] }
      range)

;; => #{ [:a 2] [:a 4] [:a 6] [:b 2] }


;; Recursive rule

(d/q '[ :find  ?u1 ?u2
        :in    $ %
        :where (follows ?u1 ?u2) ]
      [ [1 :follows 2]
        [2 :follows 3]
        [3 :follows 4] ]
     '[ [(follows ?e1 ?e2)
         [?e1 :follows ?e2]]
        [(follows ?e1 ?e2)
         [?e1 :follows ?t]
         (follows ?t ?e2)] ])

;; => #{ [1 2] [1 3] [1 4]
;;       [2 3] [2 4]
;;       [3 4] }


;; Aggregates

(d/q '[ :find ?color (max ?amount ?x) (min ?amount ?x)
        :in   [[?color ?x]] ?amount ]
     [[:red 10]  [:red 20] [:red 30] [:red 40] [:red 50]
      [:blue 7] [:blue 8]]
     3)

;; => [[:red  [30 40 50] [10 20 30]]
;;     [:blue [7 8] [7 8]]]
```

## Project status

Pre-alpha quality. I spent just one week on implementation—it’s straightforward, non-optimized and has no meaningful error reporting.

Following features are supported:

* Database as a value: each DB is an immutable value. New DBs are created on top of old ones, but old ones stay perfectly valid too
* Datomic triple store model
* EA and AV indexes
* Multi-valued attributes via `:cardinality :many`
* Database “mutations” via `transact!`
* Callback-based analogue to txReportQueue via `listen!`

Query engine supports _all_ features of Datomic Datalog:

* Implicit joins
* Query over DB or regular collections
* Parameterized queries via `:in` clause
* Tuple, collection, relation binding forms in `:in` clause
* Query over multiple DB/collections
* Predicates and user functions in query
* Rules, recursive rules
* Aggregates

Interface differences:

* Custom query functions and aggregates should be passed as source instead of being referenced by symbol (due to lack of `resolve` in CLJS)
* Conn is just an atom storing last DB value, use `@conn` instead of `(d/db conn)`
* Instead of `#db/id[:db.part/user -100]` just use `-100` in place of `:db/id` or entity id

Expected soon:

* Better error reporting
* Direct access to indexes
* Passing DB to rule
* Moar speed

## Differences from Datomic

Global differences:

* Simplified schema, not queriable
* No need to declare attributes except for `:cardinality` `:many`
* Any value can be used as entity id, attribute or value. It’s better if they are immutable and fast to compare
* No `db/ident` attributes, keywords are _literally_ attribute values, no integer id behind them
* AV index for all datoms
* No transactor functions besides `:db.fn/retractEntity` and `:db.fn/retractAttribute`
* No schema migrations
* No history support, though history can be implemented on top of immutable DB values
* No cache segments management, no laziness. All DB must reside in memory
* No facilities to persist, transfer over the wire or sync DB with the server
* No pluggable storage options, no full-text search, no partitions
* No external dependencies
* Free

Some of these are omitted intentionally. Different apps have different needs in storing/transfering/keeping track of DB state. This library is a foundation to build exactly the right storage solution for your needs without selling too much “vision”.

## License

Copyright © 2014 Nikita Prokopov

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
