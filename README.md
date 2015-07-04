<img src="https://dl.dropboxusercontent.com/u/561580/imgs/datascript_logo.svg">

> What if creating a database would be as cheap as creating a Hashmap?

An immutable in-memory database and Datalog query engine in ClojureScript.

DataScript is meant to run inside the browser. It is cheap to create, quick to query and ephemeral. You create a database on page load, put some data in it, track changes, do queries and forget about it when the user closes the page.

DataScript databases are immutable and based on persistent data structures. In fact, they’re more like data structures than databases (think Hashmap). Unlike querying a real SQL DB, when you query DataScript, it all comes down to a Hashmap lookup. Or series of lookups. Or array iteration. There’s no particular overhead to it. You put a little data in it, it’s fast. You put in a lot of data, well, at least it has indexes. That should do better than you filtering an array by hand anyway. The thing is really lightweight.

The intention with DataScript is to be a basic building block in client-side applications that needs to track a lot of state during their lifetime. There’s a lot of benefits:

- Central, uniform approach to manage all application state. Clients working with state become decoupled and independent: rendering, server sync, undo/redo do not interfere with each other.
- Immutability simplifies things even in a single-threaded browser environment. Keep track of app state evolution, rewind to any point in time, always render consistent state, sync in background without locking anybody.
- Datalog query engine to answer non-trivial questions about current app state.
- Structured format to track data coming in and out of DB. Datalog queries can be run against it too.

## Resources <a href="https://gitter.im/tonsky/datascript?utm_source=badge&amp;utm_medium=badge&amp;utm_campaign=pr-badge&amp;utm_content=badge"><img src="https://camo.githubusercontent.com/da2edb525cde1455a622c58c0effc3a90b9a181c/68747470733a2f2f6261646765732e6769747465722e696d2f4a6f696e253230436861742e737667" alt="Gitter" data-canonical-src="https://badges.gitter.im/Join%20Chat.svg" style="max-width:100%;"></a>

Blog post about [how DataScript fits into the current webdev ecosystem](http://tonsky.me/blog/decomposing-web-app-development/).

Talks:

- “DataScript for Web Development” talk (Clojure eXchange, Dec 2014): [slides](https://dl.dropboxusercontent.com/u/561580/conferences/2014.12%20clojure%20eXchange.pdf), [video](https://skillsmatter.com/skillscasts/6038-datascript-for-web-development)
- “Building ToDo list with DataScript” webinar (ClojureScript NYC, Dec 2014): [video](http://vimeo.com/114688970), [app](https://github.com/tonsky/datascript-todo)
- DataScript hangout (May 2014, in Russian): [video](http://www.youtube.com/watch?v=jhBC81pczZY)

Projects using DataScript:

- Precursor, collaborative prototyping tool: [live](https://prcrsr.com), [blog](https://prcrsr.com/blog)
- Acha-acha, github achievements: [sources](https://github.com/someteam/acha), [code walkthrough](http://tonsky.me/blog/acha-acha/), [live](http://acha-acha.co/)
- Showkr, flickr gallery viewer: [sources](https://github.com/piranha/showkr), [live](http://showkr.solovyov.net)
- Radiant, datalog sketchpad: [sources](https://github.com/robert-stuttaford/stuttaford.me/tree/master/src/stuttaford/radiant), [live version](http://www.stuttaford.me/radiant/)
- PossibleDB, server-side persistence for DataScript: [sources](https://github.com/runexec/PossibleDB)

Demo applications:

- ToDo, task manager demo app (persistence via localStorage and transit, filtering, undo/redo): [sources](https://github.com/tonsky/datascript-todo), [live](http://tonsky.me/datascript-todo/)
- CatChat, chat demo app: [sources](https://github.com/tonsky/datascript-chat), [code walkthrough](http://tonsky.me/blog/datascript-chat/), [live](http://tonsky.me/datascript-chat/)
- clj-crud, demo CRUD app: [sources](https://github.com/thegeez/clj-crud), [blog post](http://thegeez.net/2014/04/30/datascript_clojure_web_app.html)


## Usage examples [![Build Status](https://travis-ci.org/tonsky/datascript.svg?branch=master)](https://travis-ci.org/tonsky/datascript)

For more examples, see [our acceptance test suite](test/datascript/test/).

```clj
:dependencies [
  [org.clojure/clojure "1.7.0-beta2"]
  [org.clojure/clojurescript "0.0-3291"]
  [datascript "0.11.5"]
]
```

_Note:_ DataScript 0.11.3 and later requires CLJS 0.0-3291 or later

_Note:_ DataScript 0.11.2 and later requires Clojure 1.7.0-beta2 or later

_Note:_ DataScript 0.11.0 and later will only work with CLJS 0.0-3196 and later

```clj
(require '[datascript :as d])

;; Implicit join, multi-valued attribute

(let [schema {:aka {:db/cardinality :db.cardinality/many}}
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


;; Destructuring, function call, predicate call, query over collection

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

## Using from vanilla JS

DataScript can be used from any JS engine without additional dependencies:

```html
<script src="https://github.com/tonsky/datascript/releases/download/0.11.5/datascript-0.11.5.min.js"></script>
```

or as a CommonJS module ([npm page](https://www.npmjs.org/package/datascript)):

```
npm install datascript
```

```js
var ds = require('datascript');
```

or as a RequireJS module:

```js
require(['datascript'], function(ds) { ... });
```

Queries:

* Query and rules should be EDN passed as strings
* Results of `q` are returned as regular JS arrays

Entities:

* Entities returned by `entity` call are lazy as in Clojure
* Use `e.get("prop")`, `e.get(":db/id")`, `e.db` to access entity properties
* Entities implement ECMAScript 6 Map interface (has/get/keys/...)

Transactions:

* Use strings such as `":db/id"`, `":db/add"`, etc. instead of db-namespaced keywords
* Use regular JS arrays and objects to pass data to `transact` and `db_with`

Transaction reports:

* `report.tempids` has string keys (`"-1"` for entity tempid `-1`), use `resolve_tempid` to set up a correspondence

Check out [test/js/tests.html](test/js/tests.html) for usage examples.

## Project status

Alpha quality. Half of the features done, a lot of cases where error reporting is missing, no docs (use examples & Datomic documentation).

The following features are supported:

* Database as a value: each DB is an immutable value. New DBs are created on top of old ones, but old ones stay perfectly valid too
* Triple store model
* EAVT, AEVT and AVET indexes
* Multi-valued attributes via `:db/cardinality :db.cardinality/many`
* Lazy entities and `:db/valueType :db.type/ref` auto-expansion
* Database “mutations” via `transact!`
* Callback-based analogue to txReportQueue via `listen!`
* Direct index lookup and iteration via `datoms` and `seek-datoms`
* Filtered databases via `filter`
* Lookup refs
* Unique constraints, upsert
* Pull API (thx [David Thomas Hume](https://github.com/dthume))

Query engine features:

* Implicit joins
* Query over DB or regular collections
* Parameterized queries via `:in` clause
* Tuple, collection, relation binding forms in `:in` clause
* Query over multiple DB/collections
* Predicates and user functions in query
* Rules, recursive rules
* Aggregates
* Find specifications

Interface differences:

* Conn is just an atom storing last DB value, use `@conn` instead of `(d/db conn)`
* Instead of `#db/id[:db.part/user -100]` just use `-100` in place of `:db/id` or entity id
* Transactor functions can be called as `[:db.fn/call f args]` where `f` is a function reference and will take db as first argument (thx [@thegeez](https://github.com/thegeez))
* Custom query functions and aggregates should be passed as source instead of being referenced by symbol (due to lack of `resolve` in CLJS)
* Custom aggregate functions are called via aggregate keyword: `:find (aggregate ?myfn ?e) :in $ ?myfn`
* Additional `:db.fn/retractAttribute` shortcut
* Transactions are not annotated by default with `:db/txInstant`

Expected soon:

* Better error reporting
* Proper documentation
* `not`, `not-join`, `or` and `or-join` datalog clauses

## Differences from Datomic

* DataScript is built totally from scratch and is not related by any means to the popular Clojure database Datomic
* Runs in a browser
* Simplified schema, not queryable
* No need to declare attributes except for `:db/cardinality` `:db.cardinality/many` and `:db/valueType` `:db.type/ref`
* Any type can be used for values. It’s better if values are immutable and fast to compare
* No `db/ident` attributes, keywords are _literally_ attribute values, no integer id behind them
* AVET index for all datoms
* No schema migrations
* No cache segments management, no laziness. Entire DB must reside in memory
* No facilities to persist, transfer over the wire or sync DB with the server
* No pluggable storage options, no full-text search, no partitions
* No external dependencies
* Free

Aimed at interactive, long-living browser applications, DataScript DBs operate in constant space. If you do not add new entities, just update existing ones, or clean up database from time to time, memory consumption will be limited. This is unlike Datomic which keeps history of all changes, thus grows monotonically. DataScript does not track history by default, but you can do it via your own code if needed.

Some of the features are omitted intentionally. Different apps have different needs in storing/transfering/keeping track of DB state. DataScript is a foundation to build exactly the right storage solution for your needs without selling too much “vision”.

## License

Copyright © 2014–2015 Nikita Prokopov

Licensed under Eclipse Public License (see [LICENSE](LICENSE)).
