# DatomicScript

> What if creating Datomic database would be as cheap as creating an atom?

This is a port of Datomic to ClojureScript (data model and Datalog query language).

Why? Because nowadays Rich Web Apps are big enough that ad-hoc state management solutions does not work for them.

Instead of storing your app state in 1001 global vars or creating hierarchy of models inside models inside models, put it all into a database and get serverside-grade query engine on top of that.

## Work in progress [![Build Status](https://travis-ci.org/tonsky/datomicscript.svg?branch=master)](https://travis-ci.org/tonsky/datomicscript)

Following features are supported:

* Database as a value: each DB is an immutable value. New DBs are cretead on top of old ones, but old ones stays perfectly valid too
* Datomic triple store model
* EA and AV indexes
* Multi-valued attributes via schema and `:cardinality` `:many`
* Database “mutations” via `transact!`
* Callback-based analogue to txReportQueue (`listen!`)

Query engine supports _all_ features of Datomic Datalog:

* Implicit joins
* Query over DB or regular collections
* Parameterized queries via `:in` clause
* Tuple, collection, relation binding forms in `:in` clause
* Query over multiple DB/collections
* Predicates and user functions in query
* Rules, recursive rules
* Aggregates

Expected soon:

* `:db.fn/retractEntity`
* Better error reporting
* Direct access to indexes
* Passing DB to rule

## Usage examples

```clj
(require '[tonsky.datomicscript :as d])

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

## Differences from Datomic

This library is meant to run inside browser, so it must be fast to start, quick to query, single-threaded and ephemeral. You create a database on page load, put some data in it and wait for user to close the page.

Global differences:

* Simplified schema, not queriable
* No need to declare attributes except for `:cardinality` `:many`
* Any value can be used as entity id, attribute or value. It’s better if they are immutable and fast to compare
* No `db/ident` attributes, keywords are _literally_ attribute values, no integer id behind them
* AV index for all datoms
* No schema migrations
* No history support, though history can be implemented on top of immutable DB values
* No cache segments management, no laziness. All DB must reside in memory
* No facilities to persist, transfer over the wire or sync DB with the server
* No pluggable storage options
* No full-text search
* No partitions
* Free

Some of these are omitted intentionally. Different apps have different needs in storing/transfering/keeping track of DB state. This library is a foundation to build exactly the right storage solution for your needs, without selling you too much “vision”.

Interface differences:

* Custom query functions and aggregates should be passed as source instead of being referenced by symbol (due to lack of `resolve` in CLJS)
* Conn is just an atom storing last DB value, use `@conn` instead of `(d/db conn)`
* Instead of `#db/id[:db.part/user -100]` just use `-100` in place of `:db/id` or entity id
