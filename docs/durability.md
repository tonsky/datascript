# Storing DataScript on disk

There are three ways to store DataScript database on disk. First two are actually just serialization: they can turn your DB into string, which you can then store on disk or whenever you want.

## print/read

The oldest and slowest way. Works both for Clojure and ClojureScript. Just

```
(def string
  (pr-str db))
```

and later

```
(read string)
```

or, better, 

```
(clojure.edn/read-string
  {:readers d/data-readers}
  string)
```

Upsides:

- Always works.

Downsides:

- Not incremental, always stores entire db.
- EDN serialization only.
- Reading back is slow.

## serializable/from-serializable

An interesting idea I don’t think I’ve seen anywhere. Bring your own serialization. Basically when you do

```
(d/serializable db)
```

it returns a datastructure that is serialization-friendly, even for JSON (it doesn’t use keywords, for example):

```
{"count" 1, "tx0" 536870912, "max-eid" 1, "max-tx" 536870913, "schema" "nil", "attrs" [":name"], "keywords" [], "eavt" [[1 0 "Ivan" 1]], "aevt" [0], "avet" [], "branching-factor" 512, "ref-type" "soft"}
```

After that, you are free to call

```
(cheshire/generate-string
  (d/serializable db))
```

or `pr-str`, or `transit/write`. Doesn’t matter, really. Up to you. It’s also pretty convenient, because DS itself doesn’t have to depend on these serialization libraries.

When it’s time to restore, you do the same procedure in reverse:

```
(def db
  (d/from-serializable
    (cheshire/parse-string
      string)))
```

What’s good about this apporach is that it’s also faster to deserialize than print/read method. Works both in Clojure and CLJS.

Upsides:

- Works in both Clojure and ClojureScript.
- Can choose your own serialization format and implementation.
- Faster than print/read method.

Downsides:

- Not incremental, always stores entire db.

## Storage (NEW! HOT! BEST!)

This way pretends we are a real database and does things most optimally: incremental and lazy, just like the big boys. But only on JVM, at least for now. The rest of the article will discuss this approach.

Storing database is easy. First, you have to implement `datascript.storage/IStorage` protocol:

```
(def storage
  (reify datascript.storage/IStorage
    (-store [_ addr+data-seq]
      (doseq [[addr data] addr+data-seq]
        ... serialize and store <addr> -> <data> somehow ...))
    (-restore [_ addr]
      ... load and de-serialize <data> stored at <addr>)))
```

`-store` is batched so that you can, you know, wrap it in a transaction or something. `addr` is a 64-bit number. `data` is EDN-serializable data format, containing vectors, maps, integers, keywords and whatever you yourself put into values. So, not JSON-safe, but EDN-safe (as long as you don’t put weird stuff in values).

After you’re done implementing a store, call

```
(d/store! db storage)
```

and you are done!

`d/store!` is slightly mutable under-the-hood. In a sense that it remembers the storage, which nodes are stored, etc. It shouldn’t affect your regular use of a database, but still, I think you should know.

The good part about this is, if you modify your database, it’ll remember which parts of the B-tree were stored, and on a next store do an incremental update!

```
(let [db' (d/db-with db
            tx-data)]
  (d/store! db' storage))
```

The code above will do much less `-store` calls because only some parts of the tress have changed. That’s one of the main propositions of this approach.

BTW, you can also specify storage during db creation, and then call `d/store!` without storage argument:

```
(let [schema nil
      db (d/empty-db schema {:storage storage})]
  (d/store! db))
```

Just a possibility. Once stored, DB will remember its storage. Actually, storing into different storage is not supported (yet? ever?), so you can just as well start calling `(d/store! db)` without storage argument.

Ok, now for the fun stuff. How to read database from disk? Simple:

```
(def db
  (d/restore storage))
```

The fun part is, that `d/restore` does exactly zero reads! That’s right, restoring a database is lazy (and thus, super-fast). Only when you start accessing it, only relevant part will be fetched. E.g. this:

```
(first (d/datoms db))
```

Will do approximately 3-6 reads depending on how deep your db is (~ 2 + log512(datoms)).

Restored DB should work exactly as a normal DB. You can read whatever you want, query as much as you want, `entity`, `pull`, `datoms`, all should work as usual. If you modify restored DB, then `store!` it, store will be incremental as well.

And that’s it! That’s all you need to know.

## Storage + conn

Storage also work with `conn`, and, as a convenience, if you specify `:storage` when creating `conn`, it’ll then `store!` after each `transact!`:

```
(def conn
  (d/create-conn schema {:storage storage}))

(d/transact! conn
  [[:db/add 1 :name "Ivan"]]) ;; <- will be stored automatically

(d/transact! conn
  [[:db/add 2 :name "Oleg"]]) ;; <- will be stored again
```

You can restore `conn`, too:

```
(def conn
  (d/restore-conn storage))
```

## What is storage?

Important thing to understand is: one storage can only hold one database. This is because it uses constant addr to write its root. If you try to store two or more databases, last one will essentially overwrite everything else.

You can, of course, solve it on your level. If you need to store two or more databases, make a storage that uses two or more different file system directories (file storage) or two or more SQL tables (SQL storage). It’s entirely up to you.

## File Storage

DataScript comes with one default implementation of storage: `file-storage`. It takes a directory and stores everything about a database in it. Use it like this:

```
(def storage
  (d/file-storage "/tmp/db"))

(d/store db storage)

(def db'
  (d/restore storage))
```

It accepts couple of options. If you want your own serializer (it uses EDN by default):

```
:freeze-fn :: (data)   -> String. A serialization function
:thaw-fn   :: (String) -> data. A deserialization function
```

If you want to read/write from/to input/output stream yourself:

```
:write-fn  :: (OutputStream data) -> void. Implement your own writer to FileOutputStream
:read-fn   :: (InputStream) -> Object. Implement your own reader from FileInputStream
```

And finally, if you want to control how addresses are converted to file names:

```
:addr->filename-fn :: (Long) -> String. Construct file name from address
:filename->addr-fn :: (String) -> Long. Reconstruct address from file name
```

All these options are optional.

Also, remember, it’s never safe to write to file system yourself. Always do it through battle-tested layer like SQLite or RocksDB or something. If you put `file-storage` in production, expect problems :)

## Garbage collection

The IStorage protocol actually has two more methods to implement:

```
(-list-addresses [_]
  "Return seq that lists all addresses currently stored in your storage.
   Will be used during GC to remove keys that are no longer used.")
  
(-delete [_ addrs-seq]
  "Delete data stored under `addrs` (seq). Will be called during GC")
```

What’s that about?

Well, when you store your database first time, every node gets an address. Then you add or remove some stuff from it, which creates new tree, which reuses parts of the old tree. You know, good old Clojure persistent data structures (it’s not literally Clojure, I have to roll my own, but the idea is the same).

Now, new tree consists of some old reused nodes that already have addresses and some new nodes that have no addresses yet. You save it again, and new nodes gets their addresses, and old nodes are just skipped, which make the whole process efficient.

Noticed the catch? New tree lost some nodes because it no longer needs them but they are still in storage! That means eventually this kind of garbage will accumulate, and your storage will hold way more nodes than needed to build last version of database.

(you might think: going back in history? but no. That’s not how we go about that)

That’s why garbage collection exists. Your storage needs to provide us with list of all the addresses that are currently in use, and a way to delete them. Then you call:

```
(d/collect-garbage! db)
```

and you are done! It will clean up everything that is not referenced by the current DB value.

Two gotchas here:

1. It will have to read the entire DB in memory for that. So expect it to be expensive operation.
2. Any live references to _other_ databases that were lazy-loaded from the same storage might stop working.

E.g.:

```
(let [db (d/restore storage)
      db' (d/db-with db [[:db/add -1 :name "Ivan"]])]
  (d/collect-garbage! db'))
  ;; after that, db will stop working
```

We are currently thinking what to do about that.

Connection has its own version:

```
(d/collect-garbage-conn! conn)
```

Please don’t do `(d/collect-garbage! @conn)`, as conn has some extra optimization that won’t work with database version of GC.

## Options

Databases support two primary options:

```
:branching-factor <int>, default 512
```

How wide your B-trees are. By default it means that each node will contain 256...512 keys. You can change it to e.g. 1024 and have less total nodes which are bigger if e.g. writing to storage has a high overhead or something. Or to something like 64 to have 32...64 keys in each node, if you want very high granularity and it’s cheaper to write small keys in your storage.

```
:ref-type :strong | :soft | :weak, default :soft
```

This is harder to explain. DataScript consists of three B-trees. Each one is, well, a tree. We store them per-node, so each node in a tree has an address if it was previously stored (new nodes have null).

Now, if some node was read from a storage (e.g. very first leaf and all the node leading to it, if you requested `(first (d/datoms db :eavt))`, then it will store its address and its value. But the value could, at any moment, be also restore from the storage, with some deserialization penalty.

So what `:ref-type` is controlling is how values in nodes that have both address and value are stored.

`:strong` means just a normal java reference, meaning, node value will never be unloaded from memory once read. You should use it if you are absolutely sure your entire database easily fits into memory. You will still benefit from lazy loading, but once loaded, it will never unload.

`:soft` is a sweet spot. It uses `SoftReference` to store the values. It means that normally values will not be unloaded, but under memory pressure, they just might.

`:weak` is similar to `:soft`, but uses `WeakReference` and will more aggressively unload your nodes. It was intended to be used in conjunction with pluggable caches like LRU, but this have not been yet implemented. TDB.

I think default options are fine for most, but in case you need to change them, you now know how.

## Conclusion

Storing gigabytes of data in DataScript and only loading parts that are need for the query was my initial vision for DataScript from the very beginning. I’m so happy we can finally get closer to that!

Let me know if you implement something cool with it.
