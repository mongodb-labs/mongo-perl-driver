#include "perl_mongo.h"
#include "mongo_link.h"

extern int request_id;

static int already_queried(SV *self) {
  // check if the query's been executed
  return SvTRUE(perl_mongo_call_reader (self, "_queried"));
}

static mongo_cursor* get_cursor(SV *self) {
  mongo_cursor *cursor;
  buffer buf;
  mongo_msg_header header;
  int sent;

  cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);

  // if so, get the cursor
  if (cursor->started_iterating) {
    return cursor;
  }

  // if not, execute the query
  CREATE_BUF(INITIAL_BUF_SIZE);
  CREATE_HEADER_WITH_OPTS(buf, cursor->ns, OP_QUERY, cursor->opts);
  serialize_int(&buf, cursor->skip);
  serialize_int(&buf, cursor->limit);
  perl_mongo_sv_to_bson(&buf, cursor->query, "MongoDB::OID");
  if (cursor->fields) {
    perl_mongo_sv_to_bson(&buf, cursor->fields, "MongoDB::OID");
  }

  serialize_size(buf.start, &buf);

  // sends
  sent = mongo_link_say(cursor->socket, &buf);
  Safefree(buf.start);
  if (sent == -1) {
    croak("couldn't send query.");
  }

  mongo_link_hear(cursor);
  cursor->started_iterating = 1;

  return cursor;
}

static int _has_next(mongo_cursor *cursor) {
  mongo_msg_header header;
  buffer buf;
  int size;

  if ((cursor->limit > 0 && cursor->at >= cursor->limit) || 
      cursor->num == 0) {
    return 0;
  }
  else if (cursor->at < cursor->num) {
    return 1;
  }

  // we have to go and check with the db
  size = 34+strlen(cursor->ns);
  Newx(buf.start, size, char);
  buf.pos = buf.start;
  buf.end = buf.start + size;

  CREATE_RESPONSE_HEADER(buf, cursor->ns, cursor->header.request_id, OP_GET_MORE);
  serialize_int(&buf, cursor->limit);
  serialize_long(&buf, cursor->cursor_id);
  serialize_size(buf.start, &buf);

  // fails if we're out of elems
  if(mongo_link_say(cursor->socket, &buf) == -1) {
    Safefree(buf.start);
    return 0;
  }

  Safefree(buf.start);

  // if we have cursor->at == cursor->num && recv fails,
  // we're probably just out of results
  // mongo_link_hear returns 0 on success
  return (mongo_link_hear(cursor) == 0);
}


MODULE = MongoDB::Cursor  PACKAGE = MongoDB::Cursor

PROTOTYPES: DISABLE


bool
has_next (self)
        SV *self
    PREINIT:
        mongo_cursor *cursor;
    CODE:
        cursor = get_cursor(self);
        RETVAL = _has_next(cursor);
    OUTPUT:
        RETVAL

SV *
next (self)
        SV *self
    PREINIT:
        mongo_cursor *cursor;
    CODE:
        cursor = get_cursor(self);
        if (_has_next(cursor)) {
          RETVAL = perl_mongo_bson_to_sv("MongoDB::OID", &cursor->buf);
          cursor->at++;

          //TODO handle $err
	} else {
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
mongo_cursor_DESTROY (self)
      SV *self
  PREINIT:
      mongo_cursor *cursor;
  CODE:
      cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);
      Safefree(cursor);
      printf("in cursor destroy\n");
