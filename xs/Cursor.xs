#include "perl_mongo.h"

static int already_queried(SV *self) {
  // check if the query's been executed
  return SvTRUE(perl_mongo_call_reader (self, "_queried"));
}

static mongo::DBClientCursor* get_cursor(SV *self) {
  mongo::DBClientConnection *connection;
  mongo::DBClientCursor *cursor;
  mongo::BSONObj *f = 0;
  HV *this_hash;
  SV **query, **fields, **limit, **skip, **ns, **conn;

  // if so, get the cursor
  if (already_queried(self)) {
    return (mongo::DBClientCursor*)perl_mongo_get_ptr_from_instance(self);
  }

  // if not, execute the query

  this_hash = SvSTASH(SvRV(self));

  query = hv_fetch(this_hash, "query", strlen("query"), 0);
  fields = hv_fetch(this_hash, "fields", strlen("fields"), 0);
  limit = hv_fetch(this_hash, "limit", strlen("limit"), 0);
  skip = hv_fetch(this_hash, "skip", strlen("skip"), 0);

  ns = hv_fetch(this_hash, "ns", strlen("ns"), 0);
  conn = hv_fetch(this_hash, "connection", strlen("connection"), 0);

  connection = static_cast<mongo::DBClientConnection *>(perl_mongo_get_ptr_from_instance(*conn));

  if (fields) {
    f = &perl_mongo_sv_to_bson(*fields, "MongoDB::OID");
  }

  // create the cursor
  cursor = new mongo::DBClientCursor((mongo::DBConnector*)connection, 
                                     string(SvPV_nolen(*ns)), 
                                     perl_mongo_sv_to_bson(*query, "MongoDB::OID"),
                                     (int)SvIV(*limit),
                                     (int)SvIV(*skip),
                                     f,
                                     0);
  // actually do the query
  cursor->init();

  // attach to self
  perl_mongo_attach_ptr_to_instance(self, (void*)cursor);

  // set MongoDB::Cursor::_queried to 1
  perl_mongo_call_writer (self, "_queried", newSViv(1));

  return cursor;
}

MODULE = MongoDB::Cursor  PACKAGE = MongoDB::Cursor

PROTOTYPES: DISABLE


bool
has_next (self)
        SV *self
    PREINIT:
        mongo::DBClientCursor *cursor;
    CODE:
        cursor = get_cursor(self);
        RETVAL = cursor->more();
    OUTPUT:
        RETVAL

SV *
next (self)
        SV *self
    PREINIT:
        mongo::DBClientCursor *cursor;
    CODE:
        cursor = get_cursor(self);

        if (cursor->more()) {
          mongo::BSONObj obj = cursor->next();
          RETVAL = perl_mongo_bson_to_sv ("MongoDB::OID", obj);
        }
        else {
          RETVAL = &PL_sv_undef;
        }
    OUTPUT:
        RETVAL


SV *
snapshot (self)
        SV *self
    PREINIT:
        HV *this_hash;
        SV **query;
    CODE:
        if (already_queried(self)) {
          croak("cannot set snapshot() after query");
          return;
        }

        this_hash = SvSTASH(SvRV(self));
        query = hv_fetch(this_hash, "query", strlen("query"), 0);

        if (query && SvROK(*query) && SvTYPE(SvRV(*query)) == SVt_PVHV) {
          // store $snapshot
          SV **ret = hv_store((HV*)SvRV(*query), "$snapshot", strlen("$snapshot"), newSViv(1), 0);
        }
        // increment this
        SvREFCNT_inc(self);


SV *
sort (self, sort)
        SV *self
        SV *sort
     PREINIT:
        HV *this_hash;
        SV **query;
     CODE:
        if (already_queried(self)) {
          croak("cannot set sort() after query");
          return;
        }

        this_hash = SvSTASH(SvRV(self));
        query = hv_fetch(this_hash, "query", strlen("query"), 0);

        if (query && SvROK(*query) && SvTYPE(SvRV(*query)) == SVt_PVHV) {
          // store sort and increase refcount
          SV **ret = hv_store((HV*)SvRV(*query), "orderby", strlen("orderby"), SvREFCNT_inc(sort), 0);

          // if the hash update failed, decrement the refcount
          if (!ret) {
            SvREFCNT_dec(sort);
            // should we croak here?
          }
        }
        // increment this
        SvREFCNT_inc(self);



void
mongo::DBClientCursor::DESTROY ()
