# DatomicScript

> What if creating Datomic database would be as cheap as creating an atom?

This is a port of Datomic to ClojureScript (data model and Datalog query language).

Why? Because nowadays Rich Web Apps are big enough that ad-hoc state management solutions does not work for them.

Instead of storing your app state in 1001 global vars or creating hierarchy of models inside models inside models, put it all into a database and get serverside-grade query engine on top of that.

## Work in progress

Right now following features are supported:

* Database as a value: each DB is an immutable value. New DBs are cretead on top of old ones, but old ones stays perfectly valid too
* Datomic triple store model
* EA and AV indexes
* Database “mutations” via `transact`, `:add` and `:retract`
* Multi-valued attributes via schema and `:cardinality` `:many`
* Queries in Datomic Datalog format, `:find` and `:where` clauses
* Implicit joins
* Query over DB or regular collections
* Parameterized queries via `:in` clause
* Tuple, collection, relation binding forms in `:in` clause
* Query over multiple DB/collections
* Predicates and user functions in query
* Rules, recursive rules
* Aggregates

Expected:

* Simplified query syntax (vector-based)
* txReportQueue
* Better error reporting
* Direct access to indexes

## Example

```clj
(require '[tonsky.datomicscript :as d])

;; Implicit join, multi-valued attribute

(let [schema {:aka {:cardinality :many}}
      db (-> (d/create-database schema)
             (d/transact [ [:add 1 :name "Maksim"]
                           [:add 1 :age  45]
                           [:add 1 :aka  "Maks Otto von Stirlitz"]
                           [:add 1 :aka  "Jack Ryan"] ]))]
  (d/q '{:find [?n ?a]
         :where [[?e :aka "Maks Otto von Stirlitz"]
                 [?e :name ?n]
                 [?e :age  ?a]]} db))

;; => #{ ["Maksim" 45] }


;; Desctucturing, function call, predicate call

(d/q '{ :find  [ ?k ?x ]
        :in    [ [[?k [?min ?max]] ...] ?range ]
        :where [ [(?range ?min ?max) [?x ...]]
                 [(even? ?x)] ]}
      { :a [1 7], :b [2 4] }
      range)

;; => #{ [:a 2] [:a 4] [:a 6] [:b 2] }


;; Recursive rule

(d/q '{ :find  [ ?u1 ?u2 ]
        :in    [ $ % ]
        :where [ (follows ?u1 ?u2) ] }
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

(d/q '{ :find [ ?color (max ?amount ?x) (min ?amount ?x) ]
        :in [ [[?color ?x]] ?amount ] }
     [[:red 1]  [:red 2] [:red 3] [:red 4] [:red 5]
      [:blue 7] [:blue 8]]
     3))

;; => [[:red [3 4 5] [1 2 3]]
;;     [:blue [7 8] [7 8]]]
```

## Differences from Big Datomic

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

Interface differences:

* Custom query functions and aggregates should be passed as source instead of being referenced by symbol (due to lack of `resolve` in CLJS)
* DB cannot be passed to rule yet
