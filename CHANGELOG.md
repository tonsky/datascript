# WIP

- Rewritten query engine & rules engine, up to 10x times faster
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
