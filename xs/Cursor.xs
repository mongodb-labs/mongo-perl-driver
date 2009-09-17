/*
 *  Copyright 2009 10gen, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include "perl_mongo.h"
#include "mongo_link.h"

extern int request_id;

static mongo_cursor* get_cursor(SV *self) {
  SV **link_sv, *link_rv, *slave_okay;
  mongo_link *link;
  mongo_cursor *cursor;
  buffer buf;
  mongo_msg_header header;
  int sent;

  cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);

  // if so, get the cursor
  if (cursor->started_iterating) {
    return cursor;
  }

  link_sv = hv_fetch(SvSTASH(SvRV(self)), "link", strlen("link"), 0);
  link_rv = newRV_noinc(*link_sv);
  link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_rv);

  slave_okay = get_sv ("MongoDB::Cursor::slave_okay", GV_ADD);
  cursor->opts = SvIV(slave_okay) ? 1 << 2 : 0;

  // if not, execute the query
  CREATE_BUF(INITIAL_BUF_SIZE);
  CREATE_HEADER_WITH_OPTS(buf, cursor->ns, OP_QUERY, cursor->opts);
  serialize_int(&buf, cursor->skip);
  serialize_int(&buf, cursor->limit);
  perl_mongo_sv_to_bson(&buf, cursor->query, NO_PREP);
  if (cursor->fields) {
    perl_mongo_sv_to_bson(&buf, cursor->fields, NO_PREP);
  }

  serialize_size(buf.start, &buf);

  // sends
  sent = mongo_link_say(link_rv, link, &buf);
  Safefree(buf.start);
  if (sent == -1) {
    croak("couldn't send query.");
  }

  mongo_link_hear(link_rv, link, cursor);
  cursor->started_iterating = 1;

  return cursor;
}

static int _has_next(SV *self, mongo_cursor *cursor) {
  SV **link_sv, *link_rv;
  mongo_link *link;
  mongo_msg_header header;
  buffer buf;
  int size;

  if ((cursor->limit > 0 && cursor->at >= cursor->limit) || 
      cursor->num == 0 ||
      (cursor->at == cursor->num && cursor->cursor_id == 0)) {
    return 0;
  }
  else if (cursor->at < cursor->num) {
    return 1;
  }


  link_sv = hv_fetch(SvSTASH(SvRV(self)), "link", strlen("link"), 0);
  link_rv = newRV_noinc(*link_sv);
  link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_rv);

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
  if(mongo_link_say(link_rv, link, &buf) == -1) {
    Safefree(buf.start);
    return 0;
  }

  Safefree(buf.start);

  // if we have cursor->at == cursor->num && recv fails,
  // we're probably just out of results
  // mongo_link_hear returns 0 on success
  return (mongo_link_hear(link_rv, link, cursor) > 0);
}


MODULE = MongoDB::Cursor  PACKAGE = MongoDB::Cursor

PROTOTYPES: DISABLE


bool
has_next (self)
        SV *self
    PREINIT:
        mongo_cursor *cursor;
        mongo_link *link;
    CODE:
        cursor = get_cursor(self);
        RETVAL = _has_next(self, cursor);
    OUTPUT:
        RETVAL

SV *
next (self)
        SV *self
    PREINIT:
        mongo_cursor *cursor;
    CODE:
        cursor = get_cursor(self);
        if (_has_next(self, cursor)) {
          RETVAL = perl_mongo_bson_to_sv("MongoDB::OID", &cursor->buf);
          cursor->at++;

          if (cursor->num == 1 &&
              hv_exists((HV*)SvRV(RETVAL), "$err", strlen("$err"))) {
            STRLEN len;
            SV **err = hv_fetch((HV*)SvRV(RETVAL), "$err", strlen("$err"), 0);
            croak(SvPV_nolen(*err));
          }
	} else {
          RETVAL = &PL_sv_undef;
        }
    OUTPUT:
        RETVAL


SV *
snapshot (self)
        SV *self
    PREINIT:
        mongo_cursor *cursor;
        HV *this_hash;
        SV **query;
    CODE:
        cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);
        if (cursor->started_iterating) {
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
fields (self, fields)
        SV *self
        SV *fields
     PREINIT:
        mongo_cursor *cursor;
     CODE:
        cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);
        if (cursor->started_iterating) {
          croak("cannot set fields after query");
          return;
        }

        SvREFCNT_inc(fields);
        cursor->fields = fields;

        SvREFCNT_inc(self);

SV *
sort (self, sort)
        SV *self
        SV *sort
     PREINIT:
        mongo_cursor *cursor;
        HV *this_hash;
        SV **query;
     CODE:
        cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);
        if (cursor->started_iterating) {
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
        } else {
          croak("something is wrong with the query");
        }
        // increment this
        SvREFCNT_inc(self);


SV *
hint (self, hint)
        SV *self
        SV *hint
     PREINIT:
        mongo_cursor *cursor;
        HV *this_hash;
        SV **query;
     CODE:
        cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);
        if (cursor->started_iterating) {
          croak("cannot set hint() after query");
          return;
        }

        this_hash = SvSTASH(SvRV(self));
        query = hv_fetch(this_hash, "query", strlen("query"), 0);

        if (query && SvROK(*query) && SvTYPE(SvRV(*query)) == SVt_PVHV) {
          // store hint and increase refcount
          SV **ret = hv_store((HV*)SvRV(*query), "$hint", strlen("$hint"), SvREFCNT_inc(hint), 0);

          // if the hash update failed, decrement the refcount
          if (!ret) {
            SvREFCNT_dec(hint);
            // should we croak here?
          }
        } else {
          croak("something is wrong with the query");
        }
        // increment this
        SvREFCNT_inc(self);


SV *
limit (self, num)
        SV *self
        int num
     PREINIT:
        mongo_cursor *cursor;
     CODE:
        cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);
        if (cursor->started_iterating) {
          croak("cannot set limit() after query");
          return;
        }

        cursor->limit = num;
        SvREFCNT_inc(self);


SV *
skip (self, num)
        SV *self
        int num
     PREINIT:
        mongo_cursor *cursor;
     CODE:
        cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);
        if (cursor->started_iterating) {
          croak("cannot set skip() after query");
          return;
        }

        cursor->skip = num;
        SvREFCNT_inc(self);


SV *
explain (self) 
        SV *self
    PREINIT:
        mongo_cursor *cursor;
        HV *this_hash;
        SV **query;
        int temp_limit;
    CODE:
        cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);

        temp_limit = cursor->limit;
        if (cursor->limit > 0) {
          cursor->limit *= -1;
        }

        this_hash = SvSTASH(SvRV(self));
        query = hv_fetch(this_hash, "query", strlen("query"), 0);

        if (!query || !SvROK(*query) || SvTYPE(SvRV(*query)) != SVt_PVHV) {
          croak("couldn't run explain, invalid query");
        }

        // store $explain
        hv_store((HV*)SvRV(*query), "$explain", strlen("$explain"), &PL_sv_yes, 0);

        perl_mongo_call_method(self, "reset", 0);
        RETVAL = perl_mongo_call_method(self, "next", 0);

        cursor->limit = temp_limit;
    OUTPUT:
        RETVAL


SV *
reset (self)
        SV *self
    PREINIT:
        mongo_cursor *cursor;
    CODE:
        cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);
        cursor->buf.pos = cursor->buf.start;
        cursor->started_iterating = 0;
        cursor->at = 0;
        cursor->num = 0;



void
DESTROY (self)
      SV *self
  PREINIT:
      SV **link;
      HV *this_hash;
      mongo_cursor *cursor;
  CODE:
      //this_hash = SvSTASH(SvRV(self));
      //link = hv_fetch(this_hash, "link", strlen("link"), 0);
      //SvREFCNT_dec(*link);

      cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);
      if (cursor) {
        if (cursor->ns) { 
          Safefree(cursor->ns);
          cursor->ns = 0;
        }
        
        if (cursor->query) { 
          SvREFCNT_dec(cursor->query);
          cursor->query = 0;
        }
        
        if (cursor->fields) {
          SvREFCNT_dec(cursor->fields);
          cursor->fields = 0;
        }

        if (cursor->buf.start) {
          Safefree(cursor->buf.start);
          cursor->buf.start = 0;
        }
        
        Safefree(cursor);
        cursor = 0;
        
      }
