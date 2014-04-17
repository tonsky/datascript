# DatomicScript

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

Expected:

* Conditions, user functions, rules in queries
* Simplified query syntax (vector-based)
* Aggregates
* txReportQueue
* Better error reporting
* `:where` clauses order optimization (maybe)

## Example

```clj
(require '[tonsky.datomicscript :as d])

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
```

## Differences from Big Datomic

This library is meant to run inside browser, so it must be fast to start, quick to query, single-threaded and ephemeral. You create a database on page load, put some data in it and wait for user to close the page.

* No history support, though history can be implemented on top of immutable DB values
* No `transaction` attribute
* No cache segments management, no laziness. All DB must reside in-memory
* No facilities to persist, transfer over the wire or sync DB with the server
* No pluggable storage options
* No full-text search
* No partitions
* Simplified schema, no ident attributes, AV index for all datoms
* Schema required only for attributes with `:cardinality` `:many`
* Value of any type can be used for entity id, attribute or value. It’s better if they are immutable and fast to compare
* Schema not queriable
* No schema migrations
* Free
