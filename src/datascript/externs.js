var datascript = {};
datascript.db = {};

/**
 * @constructor
 */
datascript.db.Datom = function() {};
datascript.db.Datom.prototype.e;
datascript.db.Datom.prototype.a;
datascript.db.Datom.prototype.v;
datascript.db.Datom.prototype.tx;


datascript.impl = {};
datascript.impl.entity = {};

/**
 * @constructor
 */
datascript.impl.entity.Entity = function() {};
datascript.impl.entity.Entity.prototype.db;
datascript.impl.entity.Entity.prototype.eid;
datascript.impl.entity.Entity.prototype.keys      = function() {};
datascript.impl.entity.Entity.prototype.entries   = function() {};
datascript.impl.entity.Entity.prototype.values    = function() {};
datascript.impl.entity.Entity.prototype.has       = function() {};
datascript.impl.entity.Entity.prototype.get       = function() {};
datascript.impl.entity.Entity.prototype.forEach   = function() {};
datascript.impl.entity.Entity.prototype.key_set   = function() {};
datascript.impl.entity.Entity.prototype.entry_set = function() {};
datascript.impl.entity.Entity.prototype.value_set = function() {};
