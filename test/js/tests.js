var passed = 0, failures = 0, asserts = 0, errors = 0;

function eq_set(s1, s2) {
  return _.every(s1, function(e1) { return _.find(s2, function(e2) { return _.isEqual(e1, e2); }) != undefined })
      && _.every(s2, function(e2) { return _.find(s1, function(e1) { return _.isEqual(e1, e2); }) != undefined });
}

function maybe_to_datom(d) {
  if (Array.isArray(d))
    return {"e": d[0], "a": d[1], "v": d[2], "tx": d[3], "added": d[4] === undefined ? true : d[4]};
  else
    return d;
}

function cmp_datoms(d1, d2) {
  d1 = maybe_to_datom(d1);
  d2 = maybe_to_datom(d2);
  return d1.e == d2.e && d1.a == d2.a && _.isEqual(d1.v, d2.v) && d1.tx == d2.tx && d1.added == d2.added;
}

function assert_eq_datoms(expected, got, message) {
  asserts++;
  if(! _.every(_.zip(expected, got), function(dd) { return cmp_datoms(dd[0], dd[1]); }) ) {
    errors--;
    failures++;
    throw (message || "Assertion failed") + ": expected: " + JSON.stringify(expected) + ", got: " + JSON.stringify(got);
  }
}

function assert_eq(expected, got, message) {
  asserts++;
  if (!_.isEqual(expected, got)) {
    errors--;
    failures++;
    throw (message || "Assertion failed") + ": expected: " + JSON.stringify(expected) + ", got: " + JSON.stringify(got);
  }
}

function assert_ident(expected, got, message) {
  asserts++;
  if (expected !== got) {
    errors--;
    failures++;
    throw (message || "Assertion failed") + ": expected: " + JSON.stringify(expected) + ", got: " + JSON.stringify(got);
  }
}

function assert_eq_set(expected, got, message) {
  asserts++;
  if (!eq_set(expected, got)) {
    errors--;
    failures++;
    throw (message || "Assertion failed") + ": expected: " + JSON.stringify(expected) + ", got: " + JSON.stringify(got);
  }
}

function assert_eq_refs(expected, got, message) {
  var got_ids = _.map(got, function(e) { return e.get(":db/id"); });
  assert_eq_set(expected, got_ids);
}

function assert_eq_iter_set(expected, got, message) {
  var got_arr = [];
  // ECMAScript 6 iterators
  // for (let v of got) { 
  //   got_arr.push(v);
  // }
  var i = got.next();
  while (!i.done) {
    got_arr.push(i.value);
    i = got.next();
  }
  assert_eq_set(expected, got_arr, message);
}

function test_fns(fns) {
  for(var i in fns) {
    try {
      fns[i]();
      passed++;
      console.log("[ OK ] " + fns[i].name);
    } catch(e) {
      console.error(fns[i].name + ": " + e);
      errors++;
      console.error("[ FAIL ] " + fns[i].name);
    }
  }
  
  console.log("Ran " + fns.length + " tests containing " + asserts + " assertions.");
  console.log("Testing complete: " + failures + " failures, " + errors + " errors."); 
  
  return { fail:  failures,
           error: errors,
           test:  fns.length,
           pass:  asserts - failures - errors };
}

var d = datascript.js;
var tx0 = 0x20000000; // we just know it, alright?

function test_db_with() {
  var db = d.empty_db();
  var db1 = d.db_with(db, [[":db/add", 1, "name", "Ivan"],
                           [":db/add", 1, "age", 17]]);
  var db2 = d.db_with(db1, [{":db/id": 2,
                            "name": "Igor",
                            "age": 35}]);
  var q = '[:find ?n ?a :where [?e "name" ?n] [?e "age" ?a]]'; 
  assert_eq_set([["Ivan", 17]], d.q(q, db1));
  assert_eq_set([["Ivan", 17], ["Igor", 35]], d.q(q, db2));
}

function test_nested_maps() {
  var q = '[:find ?e ?a ?v :where [?e ?a ?v]]';
  
  var db0 = d.empty_db({"profile": {":db/valueType": ":db.type/ref"}});
  var db = d.db_with(db0, [{"name": "Igor", "profile": {"email": "@2"} }]);
  assert_eq_set([[1, "name", "Igor"], [1, "profile", 2], [2, "email", "@2"]], d.q(q, db));
  
  db = d.db_with(db0, [{"email": "@2", "_profile": {"name": "Igor"}}]);
  assert_eq_set([[1, "email", "@2"], [2, "name", "Igor"], [2, "profile", 1]], d.q(q, db));
  
  db0 = d.empty_db({"user/profile": {":db/valueType": ":db.type/ref"}});
  db = d.db_with(db0, [{"name": "Igor", "user/profile": {"email": "@2"} }]);
  assert_eq_set([[1, "name", "Igor"], [1, "user/profile", 2], [2, "email", "@2"]], d.q(q, db));
  
  db = d.db_with(db0, [{"email": "@2", "user/_profile": {"name": "Igor"}}]);
  assert_eq_set([[1, "email", "@2"], [2, "name", "Igor"], [2, "user/profile", 1]], d.q(q, db));
}

function test_init_db() {
  var q = '[:find ?n ?a ?tx :where [?e "name" ?n ?tx] [?e "age" ?a]]'; 
  var db_ok = function(db) {
    assert_eq_set([["Ivan", 17, tx0],
                   ["Igor", 35, tx0+1]],
                  d.q(q, db));
  };
  db_ok(d.init_db([[1, "name", "Ivan"],
                   [1, "age", 17],
                   [2, "name", "Igor", tx0+1],
                   [2, "age", 35, tx0+1]]));
  db_ok(d.init_db([{e: 1, a: "name", v: "Ivan"},
                   {e: 1, a: "age", v: 17},
                   {e: 2, a: "name", v: "Igor", tx: tx0+1},
                   {e: 2, a: "age", v: 35, tx: tx0+1}]));
  var db = d.init_db([[1, "aka", "X"],
                      [1, "aka", "Y"]],
                     {"aka": {":db/cardinality": ":db.cardinality/many"}});
  assert_eq_set(["X", "Y"], d.q('[:find [?aka ...] :where [_ "aka" ?aka]]', db));
}

function test_dbfn_call() {
  var dbfn = function(db, e, n, a) { 
    return [[":db/add", e, "name", n],
            [":db/add", e, "age", a]]; 
  }
  var db = d.db_with(d.empty_db(), [[":db.fn/call", dbfn, 1, "Ilya", 44]]);
  var q = '[:find ?n ?a :where [?e "name" ?n] [?e "age" ?a]]'; 
  assert_eq_set([["Ilya", 44]], d.q(q, db));
}

function test_schema() {
  var schema = {"aka": {":db/cardinality": ":db.cardinality/many"}};
  var db = d.db_with(d.empty_db(schema), 
                         [[":db/add", -1, "name", "Ivan"],
                          [":db/add", -1, "aka", "X"],
                          [":db/add", -1, "aka", "Y"],
                          {":db/id": -2,
                           "name": "Igor",
                           "aka": ["F", "G"]}]);
  var q = '[:find ?aka :in $ ?e :where [?e "aka" ?aka]]'; 
  assert_eq_set([["X"], ["Y"]], d.q(q, db, 1));
  assert_eq_set([["F"], ["G"]], d.q(q, db, 2));
}

function test_tx_report() {
  var conn = d.create_conn();
  var log = [];
  var meta = [];
  d.listen(conn, function(report) { log.push(report.tx_data); 
                                    meta.push(report.tx_meta); });
  var tx_report = d.transact(conn, [[":db/add", -1, "name", "Ivan"],
                                    [":db/add", -1, "age", 17]], {"some-meta": 1});
  assert_eq_datoms([[1, "name", "Ivan", tx0+1],
                    [1, "age", 17, tx0+1]],
                   tx_report.tx_data);
  assert_eq({"-1": 1, ":db/current-tx": tx0+1}, tx_report.tempids);
  assert_eq(1, d.resolve_tempid(tx_report.tempids, -1));
  assert_eq(tx0+1, d.resolve_tempid(tx_report.tempids, ":db/current-tx"));
  assert_eq_datoms([[1, "name", "Ivan", tx0+1],
                    [1, "age", 17, tx0+1]],
                    log[0]);
  assert_eq(tx_report.tx_meta, {"some-meta": 1});
  assert_eq(meta[0], {"some-meta": 1});
}

function test_entity() {
  var schema = {"aka": {":db/cardinality": ":db.cardinality/many"}};
  var db = d.db_with(d.empty_db(schema), 
                         [{":db/id": 1,
                           "name": "Ivan",
                           "aka": ["X", "Y"]},
                          {":db/id": 2}]);
  var e = d.entity(db, 1);
  assert_eq("Ivan",     e.get("name"));
  assert_eq(["X", "Y"], e.get("aka"));
  assert_eq(1,          e.get(":db/id"));
  
  assert_ident(db, e.db);
  
  var e2 = d.entity(db, 2);
  assert_eq(null, e2.get("name"));
  assert_eq(null, e2.get("aka"));
  assert_eq(2,    e2.get(":db/id"));

  // js interop
  assert_eq_set(["name", "aka"], e.key_set());
  assert_eq_set(["Ivan", ["X", "Y"]], e.value_set());
  assert_eq_set([["name", "Ivan"], ["aka", ["X", "Y"]]], e.entry_set());
  
  var foreach = [];
  e.forEach(function(v, a) { foreach.push([a,v]); });
  assert_eq_set([["name", "Ivan"], ["aka", ["X", "Y"]]], foreach);
  
  foreach = [];
  e.forEach(function(v, a) { this.push([a,v]); }, foreach);
  assert_eq_set([["name", "Ivan"], ["aka", ["X", "Y"]]], foreach);
  
  // js/map interfaces
  assert_eq_iter_set(["name", "aka"], e.keys());
  assert_eq_iter_set(["Ivan", ["X", "Y"]], e.values());
  assert_eq_iter_set([["name", "Ivan"], ["aka", ["X", "Y"]]], e.entries());
}

function test_entity_refs() {
  var schema = {"father":   {":db/valueType":   ":db.type/ref"},
                "children": {":db/valueType":   ":db.type/ref",
                             ":db/cardinality": ":db.cardinality/many"}};
  var db = d.db_with(d.empty_db(schema), 
                         [{":db/id": 1,   "children": [10]},
                          {":db/id": 10,  "father":   1, "children": [100, 101]},
                          {":db/id": 100, "father":   10}]);
  
  var e = function(id) { return d.entity(db, id); };
  
  assert_eq_refs([10], e(1).get("children"));
  assert_eq_refs([101, 100], e(10).get("children"));
  
  // empty attribute
  assert_eq(null, e(100).get("children"));
  
  // nested navigation
  assert_eq_refs([100, 101], e(1).get("children")[0].get("children"));
  assert_eq     (10,         e(10).get("children")[0].get("father").get(":db/id"));
  assert_eq_refs([10],       e(10).get("father").get("children"));
  
  // backward navigation
  assert_eq     (null,  e(1).get("_children"));
  assert_eq_refs([10],  e(1).get("_father"));
  assert_eq_refs([1],   e(10).get("_children"));
  assert_eq_refs([100], e(10).get("_father"));
  assert_eq_refs([1],   e(100).get("_children")[0].get("_children"));
}

function test_pull() {
  var schema = {"father":   {":db/valueType":   ":db.type/ref"},
                "children": {":db/valueType":   ":db.type/ref",
                             ":db/cardinality": ":db.cardinality/many"}};
  var db = d.db_with(d.empty_db(schema),
                         [{":db/id": 1,   "name": "Ivan", "children": [10]},
                          {":db/id": 10,  "father":   1, "children": [100, 101]},
                          {":db/id": 100, "father":   10}]);

  var actual, expected;

  actual   = d.pull(db, '["children"]', 1);
  expected = {"children": [{":db/id": 10}]};
  assert_eq(expected, actual);

  actual   = d.pull(db, '["children", {"father" ["name" :db/id]}]', 10);
  expected = {"children": [{":db/id": 100}, {":db/id": 101}],
              "father": {"name": "Ivan", ":db/id": 1}};
  assert_eq(expected, actual);
}

function test_resolve_current_tx() {
  var schema = {"created-at": {":db/valueType":   ":db.type/ref"}};
  var conn = d.create_conn(schema);
  var tx_report = d.transact(conn, [{"name": "X", "created-at": ":db/current-tx"},
                                    {":db/id": ":db/current-tx", "prop1": "val1"},
                                    [":db/add", ":db/current-tx", "prop2", "val2"],
                                    [":db/add", -1, "name", "Y"],
                                    [":db/add", -1, "created-at", ":db/current-tx"]]);
  var tx = tx_report.tempids[":db/current-tx"];
  assert_eq(tx0+1, tx);
  assert_eq_datoms(
    [[1, "created-at", tx,     tx],
     [1, "name",       "X",    tx],
     [2, "created-at", tx,     tx],
     [2, "name",       "Y",    tx],
     [tx, "prop1",     "val1", tx],
     [tx, "prop2",     "val2", tx]],
    d.datoms(d.db(conn), ":eavt"));
}


var people_db = d.db_with(d.empty_db(),
                 [{ ":db/id": 1, "name": "Ivan", "age": 15 },
                  { ":db/id": 2, "name": "Petr", "age": 37 },
                  { ":db/id": 3, "name": "Ivan", "age": 37 }]);

function test_q_relation() {
  var res = d.q('[:find ?e ?email \
                  :in    $ $b \
                  :where [?e "name" ?n] \
                         [$b ?n ?email]]',
                people_db,
              [["Ivan", "ivan@mail.ru"],
               ["Petr", "petr@gmail.com"]]);
  assert_eq_set([[1, "ivan@mail.ru"], [2, "petr@gmail.com"], [3, "ivan@mail.ru"]], res);

  res     = d.q('[:find ?e ?email \
                  :in    $ [[?n ?email]] \
                  :where [?e "name" ?n]]',
                people_db,
              [["Ivan", "ivan@mail.ru"],
               ["Petr", "petr@gmail.com"]]);
  assert_eq_set([[1, "ivan@mail.ru"], [2, "petr@gmail.com"], [3, "ivan@mail.ru"]], res);
}

function test_q_rules() {
  var res = d.q('[:find ?e1 ?e2 \
                  :in    $ % \
                  :where (mate ?e1 ?e2) \
                         [(< ?e1 ?e2)]]',
                people_db,
                '[[(mate ?e1 ?e2)   \
                   [?e1 "name" ?n]  \
                   [?e2 "name" ?n]] \
                  [(mate ?e1 ?e2)   \
                   [?e1 "age" ?a]   \
                   [?e2 "age" ?a]]]');
  assert_eq_set([[1, 3], [2, 3]], res);
}

function test_q_fns() {
  var res = d.q('[:find ?e \
                  :in    $ ?adult \
                  :where [?e "age" ?a] \
                         [(?adult ?a)]]',
                people_db,
                function(a) { return a > 18; });
  assert_eq_set([[2], [3]], res);
}

function test_find_specs() {
  var res = d.q('[:find [?name ...] \
                  :where [_ "name" ?name]]',
                people_db);
  assert_eq(["Ivan", "Petr"], res);
  
  var res = d.q('[:find [?name ?age] \
                  :where [1 "name" ?name] \
                         [1 "age" ?age]]',
                people_db);
  assert_eq(["Ivan", 15], res);
  
  var res = d.q('[:find ?name . \
                  :where [1 "name" ?name]]',
                people_db);
  assert_eq("Ivan", res);
}

function test_datoms() {
  assert_eq_datoms([[1, "age", 15, tx0+1],
                    [1, "name", "Ivan", tx0+1]],
                   d.datoms(people_db, ":eavt", 1));
  
  assert_eq_datoms([[2, "age", 37, tx0+1],
                    [3, "age", 37, tx0+1],
                    [1, "name", "Ivan", tx0+1],
                    [3, "name", "Ivan", tx0+1],
                    [2, "name", "Petr", tx0+1]],
                   d.seek_datoms(people_db, ":avet", "age", 20));
}

function test_filter() {
  assert_eq_set([[1, "name", "Ivan"],
                 [2, "name", "Petr"],
                 [3, "name", "Ivan"]],
                d.q("[:find ?e ?a ?v :where [?e ?a ?v]]",
                    d.filter(people_db,
                             function(db,datom) { return datom.a == "name"; })));
  
  assert_eq_set([[1, "name", "Ivan"],
                 [1, "age", 15]],
                d.q("[:find ?e ?a ?v :where [?e ?a ?v]]",
                    d.filter(people_db,
                             function(db,datom) { 
                               var entity = d.entity(db, datom.e);
                               return entity.get("age") <= 18;
                             })));
}

function test_datascript_js() {
  return test_fns([ test_db_with,
                    test_nested_maps,
                    test_init_db,
                    test_dbfn_call,
                    test_schema,
                    test_tx_report,
                    test_entity,
                    test_entity_refs,
                    test_pull,
                    test_resolve_current_tx,
                    test_q_relation,
                    test_q_rules,
                    test_q_fns,
                    test_find_specs,
                    test_datoms,
                    test_filter
                  ]);
}
