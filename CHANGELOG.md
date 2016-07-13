# WIP

- large long entity ids were breaking `init-db` (issue #165)

# 0.15.1

- `get-else` throws if `nil` is used for default value
- JS API: Do not keywordize attribute names in schema (PR #155, issue #153, thx [typeetfunc](https://github.com/typeetfunc))
- `init-db` sets correct `max-eid` when processing facts about transactions (PR #164, issue #163, thx [Nick Alexander](https://github.com/ncalexan))

# 0.15.0

- Better error messages for upsert conflicts
- Backtracking of tempids when redefining them later with upserts (issue #76)
- Upsert works in vector form too (issue #99, #109)
- Can specify transaction number in `:db/add`
- Can put datoms into transaction directly (issue #121: supports both addition and retration datoms, keeps tx number)
- Added all `datascript.core` symbols to externs so they can be called directly from JS (e.g. in combination with mori, issue #139)
- Added `reset-conn`, `conn-from-datoms` and `conn-from-db` (issue #45)

# 0.14.0

- Unify fn binding results with existing relations (PR #138, issue #141, thx [Aaron Brooks](https://github.com/abrooks))
- [ BREAKING ] `get-some` returns `[attr value]` instead of just `value` (#143, same as Datomic)
- [ BREAKING ] Returning `nil` from query function filters out tuple from result
- Throw when calling unbound predicate/function inside a query (#111)
- Added several built-ins, including `count` (#142) and `subs` (#111)
- Lookups with nil value (`[<attr> nil]`) resolve to nil (#140)
- Allow pull-pattern to be specified with a input variable without `?` prefix (#136, #122)
- Retract functions do not fail if lookup ref is pointing nowhere (#131)
- Fixed BTSet implementation of IReduceInit (#124)

# 0.13.3

- Accept `nil` as old value in `:db.fn/cas` (PR #127, thx [Petter Eriksson](https://github.com/petterik))

# 0.13.2

- Fixed issue #120 with binding of large collections

# 0.13.1

- Guard `conn?` will check that value is derefable instead of checking for `(instance? Atom)`

# 0.13.0

- **[ BREAKING ] Main namespace to include is now `datascript.core`, not `datascript`**
- [ BREAKING ] Old `datascript.core` (internal namespace) was renamed to `datascript.db`
- [ BREAKING ] `datascript.shim` (internal namespace) was renamed to `datascript.arrays`

Motivations:

- Usage of top-level namespaces is discouraged and even generates a warning in CLJS.
- Better sooner than later.
- 0.13 seems like a great number for this sort of release.

Migration procedure:

- Just change `(require '[datascript :as d])` to `(require '[datascript.core :as d])` and you’re good to go.

For the sake of easy migration, there’re no other changes in this release. Just renamings.

# 0.12.2

- Fix null pointer exception when `contains?` is called with an entity (PR #114, thx [Kevin Lynagh](https://github.com/lynaghk))

# 0.12.1

- `db-init` respects `:db/index` property

# 0.12.0

[ BREAKING ] Introducing new `:db/index` schema attribute:

- Attributes are not put to AVET _by default_ anymore
- Following attributes are put to AVET:

  - `:db/index true`
  - `:db/unique :db.unique/identity`
  - `:db/unique :db.unique/value`
  - `:db/valueType :db.type/ref`

- All attributes put to AVET should be comparable. Note: maps and list are not comparable by default, vectors are compared value-by-value
- [ BREAKING ] Min/max aggregation functions will only work on a comaparable values

Benefits:

- You can finally store _any_ trash easily and reliably in DataScript database: maps, vectors, functions, JS objects. It doesn’t even have to be comparable. Just do not mark it as `:db/index` and do not make it `:db.cardinality/many`
- Faster transactions. There’s ⅓ less indexes to fill
- Good defaults. Most cases where you’ll probably use AVET (lookup by value)—lookup refs, external ids, references—they are all indexed by default

Caveats:

- Your code may break (see below)
- Queries can do lookup by value event without `:db/index` attribute, but it’ll be slower

Migration procedure:

- Check your code for `datoms`, `seek-datoms` calls with `:avet` index, and `index-range` call
- Check your queries to see if they utilize AVET index. It happens when you use this pattern: `[?free-var <constant-attr> <constant-value>]`
- Mark necessary attributes with `:db/index true` in the schema

# 0.11.6

- msec argument to squuid (#95, #97)
- lookup refs in JS API `pull`, `pull_many`, `entity` (#94, thx [Matt Senior](https://github.com/mattsenior))
- fix in Pull API for reverse non-component attributes (#91, thx [Matt Senior](https://github.com/mattsenior))
- Node.js and Browser repls for dev profile (#93)
- Preconditions to validate db/conn arguments (#101)
- Id allocation bug (#66)

# 0.11.5

- Ported BTSet to JVM
- BTSet and BTSetIter implement ChunkedSeq
- New benchmark runner

This release brings a significant performance boost for JVM version of DataScript (numbers are in comparison to JS/v8 version):

- queries with single clause, no join: ~5–6 times faster
- queries with joins: ~3–4.5 times faster
- `transact`: ~3 times faster
- `init-db`: ~3–4 times faster
- rules: ~3-4 times faster

# 0.11.4

Experimental support for Clojure on JVM:

  - `sorted-set` instead of BTSet
  - polymorphic `get` lookup in query
  - no sorting optimisation for `init-db`

Performance numbers so far ([raw data](https://gist.github.com/tonsky/9da339f87113e1f1a395)):

- queries with single clause, no join: 30–50% faster 
- queries with joins: ~50% slower
- `transact`: ~3.5–4 times faster
- `init-db`: ~2–2.5 times slower
- rules: ~2–5 times faster
- db `equiv`: ~5 times slower
- db `hash`: ~10 times slower

# 0.11.3

- Fixed UUID constructor warning under CLJS 0.0-3291 and later

# 0.11.2

- Support reverse attribute refs in combination with wildcards (issue #80)
- `*.cljc` sources do not conflict with Clojure now. Code compiles under `[lein-cljsbuild "1.0.6"]`

# 0.11.1

- Return nil from entity when passed nil eid (issue #75)

# 0.11.0

- Fixed `init_db` in js (issue #73)
- Preliminary work for Clojure port (PR #70, #71)
- [ BREAKING ] DS now requires CLJS 0.0-3196 or later

# 0.10.0

- New parser for query and rules
- Added a lot of meaningful error messages and validations to query parser
- Support for _ (ignore binding) in `:in` and `fn-clause` bindings
- Fixed id allocation bug when nested maps are used for referenced entities (issue #59)
- Fixed squuids omitting leading zeros (issue #60)
- Fixed a bug when cross-referencing components caused infinite loop in Pull API (isuue #58, pull request #61)
- Pull API handles recursion more like Datomic does (issue #62)
- Exposed DB filter API to js (pull request #65)
- Added ICounted, ISequable, IEmptyableCollection to `datascript.core/DB`
- Force put `Datom` and `DB` tag readers to `cljs.reader/*tag-table*`, do not rely on `data_readers.clj`

# 0.9.0

- Lookup refs can be inlined in query `:where` patterns (issue #53)
- Pull API (issue #37, pull request #51) by [David Thomas Hume](https://github.com/dthume)
- `pull` form in `:find` clause

# 0.8.1

- `:db/unique` constraints 
- Upserts
- Lookup refs
- `entid` function

# 0.8.0

- Find specifications: collection `:find [?e ...]`, tuple `:find [?e ?v]`, and scalar `:find ?e .`
- Support for `:db/isComponent` (issue #3)
- Support for nested maps in `transact!` (also fixes #38)
- [ BREAKING ] Custom aggregate fns must be called via special syntax (`aggregate` keyword): `(q '[:find (aggregate ?myfn ?e) :in $ ?myfn ...])`. Built-in aggregates work as before: `(q '[:find (count ?e) ...]`
- Return nil from `entity` when passed nil eid (issue #47)
- Transaction data is now validated, with proper error messages (also fixes #48)

# 0.7.2

- Fixed a bug with function call after function relation was filtered out (issue #44) 

# 0.7.1

- Fixed a bug when emtpy coll in input was not creating empty relation

# 0.7.0

- BTSet and its slices (returned by `datoms`/`seek-datoms`/`index-range` calls) now support fast reverse iteration via `reverse`.
- `(datascript/datom e a v & [tx added])` call for creating new datoms
- Added missing aggregate funs: `avg`, `median`, `variance`, `stddev` and `count-distinct` (issue #42, thx [@montyxcantsin](https://github.com/montyxcantsin))
- `min` and `max` aggregates use comparator instead of default js `<` comparison
- Fixed a bug when fn inside a query on empty relation returned non-empty result
- Filtered DB support via `filter`

# 0.6.0

- [ BREAKING ] Entity ids and transaction ids now have to be numbers, attributes can only be keywords (strings if used from JS)
- New method `init-db` for fast creation of DB from existing datoms (useful for e.g. initialization from serialized state)
- Improved performance of `transact!` (~20%) and initial database population (`init-db`, `db-from-reader`, ~800%)

# 0.5.2

- Externs provided via `deps.cljs` — no need to manually specify externs when using datascript dependency

# 0.5.1

- Ability to pass inputs (predicates/functions bindings) down to rules (issues #28 #29)

# 0.5.0

- Javascript version is now packaged as a proper CommonJS/RequireJS module (include via script tag still supported) (issue #39)
- Published to npm: [npmjs.org/package/datascript](https://www.npmjs.org/package/datascript)
- [ BREAKING ] Javascript namespace is renamed from `datascript.js` to `datascript`

# 0.4.2

- Reference transaction id inside transaction by using `:db/current-tx` instead of entity id (issue #32)
- You can get id of transaction from `TxReport`: `(get-in tx-report [:tempids :db/current-tx])`
- Ability to pass arbitrary metadata along with transaction (third argument to `transact!`) and read it in listener callback (`:tx-meta` key of `TxReport`) (issue #32)

# 0.4.1

- Added `:db.fn/cas` transaction function (issue #20, thx [@montyxcantsin](https://github.com/montyxcantsin))
- Added `get-some`, `get-else`, `misssing?` query functions (issue #21, thx [@montyxcantsin](https://github.com/montyxcantsin))
- Fixed `touch` breaking entity cache (issue #17)
- Added `ground` fn (issue #25)
- Fixed query predicate/fn calls without free variables
- Retract incoming entity references on `:db.fn/retractEntity` (issue #18)
- You can now use reverse relation name when transacting entity as a map

# 0.4.0

Cosmetic changes to better mimic Datomic API. Useful for sharing code between Datomic and DataScript:

- Added `tempid`, `resolve-tempid`, `db`, `transact`, `transact-async`, `index-range`, `squuid`, `squuid-time-millis`
- [ BREAKING ] renamed `transact` to `with`, `with` to `db-with`

# 0.3.1

- Optimized speed of DB’s `equiv` and `hash`, Datom’s `hash`
- Entity’s `touch` call accessible through `datascript` namespace
- Accept sets in entity maps as values for `:db.cardinality/many` attributes

# 0.3.0

Proper entities implementation:

- Entities are now lazy and implement usual Map protocols
- [ BREAKING ] When accessing attribute of `:db/valueType :db.type/ref`, its value will be automatically expanded to entites, allowing for recursive exploration of entities graphs (e.g. `(-> (d/entity db 42) :parent :parent :children)`)
- Entities support backwards navigation (e.g. `(:person/_friends (d/entity db 42))`)

# 0.2.1

- Externs file now can be referred as `:externs [datascript/externs.js"]`

# 0.2.0

Big performance improvements:

- New B-tree based indexes
- New set-at-a-time, hash-join powered query and rules engine
- Queries now up to 10× times faster
- You can specify DB for rule call (like `($db follows ?e1 ?e2)`)
- Datoms are required to have integer id and keyword attributes, but no restriction on types of values

# 0.1.6

- Clojure reader support (pr/read) for DB and Datom

# 0.1.5

- `datoms` and `seek-datoms` API calls
- referencing other entities’ tempids in transaction data (issue #10)

# 0.1.4

- Transactor functions via `:db.fn/call` (thx [@thegeez](https://github.com/thegeez))
- Vanilla JS API bindings
- [ BREAKING ] Schema keywords namespaced on a par with Datomic schema

# 0.1.3

- `entity` added
