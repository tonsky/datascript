# DataScript

An immutable in-memory database and Datalog query engine for JS engines.

DataScript is meant to run inside the browser. It is cheap to create, quick to query and ephemeral. You create a database on page load, put some data in it, track changes, do queries and forget about it when the user closes the page.

DataScript databases are immutable and based on persistent data structures. In fact, they’re more like data structures than databases (think Hashmap). Unlike querying a real SQL DB, when you query DataScript, it all comes down to a Hashmap lookup. Or series of lookups. Or array iteration. There’s no particular overhead to it. You put a little data in it, it’s fast. You put in a lot of data, well, at least it has indexes. That should do better than you filtering an array by hand anyway. The thing is really lightweight.

The intention with DataScript is to be a basic building block in client-side applications that needs to track a lot of state during their lifetime. There’s a lot of benefits:

- Central, uniform approach to manage all application state. Clients working with state become decoupled and independent: rendering, server sync, undo/redo do not interfere with each other.
- Immutability simplifies things even in a single-threaded browser environment. Keep track of app state evolution, rewind to any point in time, always render consistent state, sync in background without locking anybody.
- Datalog query engine to answer non-trivial questions about current app state.
- Structured format to track data coming in and out of DB. Datalog queries can be run against it too.

For more info, check out [project page](https://github.com/tonsky/datascript)
