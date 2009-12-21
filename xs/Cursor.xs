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

static mongo_cursor* get_cursor(SV *self);
static int has_next(SV *self, mongo_cursor *cursor);
static void kill_cursor(SV *self);

static mongo_cursor* get_cursor(SV *self) {
  SV *link, *slave_okay, *skip, *limit,
     *query, *fields, *ns, *started_iterating;
  mongo_cursor *cursor;
  buffer buf;
  mongo_msg_header header;
  int sent, opts = 0;

  cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);

  started_iterating = perl_mongo_call_reader (self, "started_iterating");

  // if so, get the cursor
  if (SvIV(started_iterating)) {
    SvREFCNT_dec(started_iterating);
    return cursor;
  }
  SvREFCNT_dec(started_iterating);

  link = perl_mongo_call_reader (self, "_connection");
  ns = perl_mongo_call_reader (self, "_ns");
  skip = perl_mongo_call_reader (self, "_skip");
  limit = perl_mongo_call_reader (self, "_limit");
  query = perl_mongo_call_reader (self, "_query");
  fields = perl_mongo_call_reader (self, "_fields");

  slave_okay = get_sv ("MongoDB::Cursor::slave_okay", GV_ADD);
  opts = SvTRUE(slave_okay) ? 1 << 2 : 0;

  // if not, execute the query
  CREATE_BUF(INITIAL_BUF_SIZE);
  CREATE_HEADER_WITH_OPTS(buf, SvPV_nolen(ns), OP_QUERY, opts);
  perl_mongo_serialize_int(&buf, SvIV(skip));
  perl_mongo_serialize_int(&buf, SvIV(limit));
  perl_mongo_sv_to_bson(&buf, query, NO_PREP);
  if (SvROK(fields)) {
    perl_mongo_sv_to_bson(&buf, fields, NO_PREP);
  }

  perl_mongo_serialize_size(buf.start, &buf);

  SvREFCNT_dec(ns);
  SvREFCNT_dec(query);
  SvREFCNT_dec(fields);
  SvREFCNT_dec(limit);
  SvREFCNT_dec(skip);

  // sends
  sent = mongo_link_say(link, &buf);
  Safefree(buf.start);
  if (sent == -1) {
    SvREFCNT_dec(link);
    croak("couldn't send query.");
  }

  mongo_link_hear(self);

  started_iterating = perl_mongo_call_method (self, "started_iterating", 1, sv_2mortal(newSViv(1)));
  SvREFCNT_dec(started_iterating);
  SvREFCNT_dec(link);

  return cursor;
}

static int has_next(SV *self, mongo_cursor *cursor) {
  SV *link, *limit, *ns;
  mongo_msg_header header;
  buffer buf;
  int size, heard;

  limit = perl_mongo_call_reader (self, "_limit");

  if ((SvIV(limit) > 0 && cursor->at >= SvIV(limit)) || 
      cursor->num == 0 ||
      (cursor->at == cursor->num && cursor->cursor_id == 0)) {
    SvREFCNT_dec(limit);
    return 0;
  }
  else if (cursor->at < cursor->num) {
    SvREFCNT_dec(limit);
    return 1;
  }


  link = perl_mongo_call_reader (self, "_connection");
  ns = perl_mongo_call_reader (self, "_ns");

  // we have to go and check with the db
  size = 34+strlen(SvPV_nolen(ns));
  New(0, buf.start, size, char);
  buf.pos = buf.start;
  buf.end = buf.start + size;

  CREATE_RESPONSE_HEADER(buf, SvPV_nolen(ns), cursor->header.request_id, OP_GET_MORE);
  perl_mongo_serialize_int(&buf, SvIV(limit));
  perl_mongo_serialize_long(&buf, cursor->cursor_id);
  perl_mongo_serialize_size(buf.start, &buf);

  SvREFCNT_dec(limit);
  SvREFCNT_dec(ns);

  // fails if we're out of elems
  if(mongo_link_say(link, &buf) == -1) {
    SvREFCNT_dec(link);
    Safefree(buf.start);
    return 0;
  }

  Safefree(buf.start);

  // if we have cursor->at == cursor->num && recv fails,
  // we're probably just out of results
  // mongo_link_hear returns 0 on success
  heard = mongo_link_hear(self);
  SvREFCNT_dec(link);
  return heard > 0;
}


static void kill_cursor(SV *self) {
  mongo_cursor *cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);
  SV *link = perl_mongo_call_reader (self, "_connection");
  char quickbuf[128];
  buffer buf;
  mongo_msg_header header;

  // we allocate a cursor even if no results are returned,
  // but the database will throw an assertion if we try to
  // kill a non-existant cursor
  // non-cursors have ids of 0
  if (cursor->cursor_id == 0) {
    SvREFCNT_dec(link);
    return;
  }
  buf.pos = quickbuf;
  buf.start = buf.pos;
  buf.end = buf.start + 128;

  // std header
  CREATE_MSG_HEADER(cursor->header.request_id++, 0, OP_KILL_CURSORS);
  APPEND_HEADER(buf, 0);

  // # of cursors
  perl_mongo_serialize_int(&buf, 1);
  // cursor ids
  perl_mongo_serialize_long(&buf, cursor->cursor_id);
  perl_mongo_serialize_size(buf.start, &buf);

  mongo_link_say(link, &buf);
  SvREFCNT_dec(link);
}


MODULE = MongoDB::Cursor  PACKAGE = MongoDB::Cursor

PROTOTYPES: DISABLE

void
_init (self)
        SV *self
    PREINIT:
        mongo_cursor *cursor;
    CODE:
        New(0, cursor, 1, mongo_cursor);
        cursor->started_iterating = 0;

	// zero results fields
	cursor->num = 0;
	cursor->at = 0;

        // clear the buf
        cursor->buf.start = 0;
        cursor->buf.pos = 0;
        cursor->buf.end = 0;

        // attach a mongo_cursor* to the MongoDB::Cursor
        perl_mongo_attach_ptr_to_instance(self, cursor);



bool
has_next (self)
        SV *self
    PREINIT:
        mongo_cursor *cursor;
        mongo_link *link;
    CODE:
        cursor = get_cursor(self);
        RETVAL = has_next(self, cursor);
    OUTPUT:
        RETVAL

SV *
next (self)
        SV *self
    PREINIT:
        mongo_cursor *cursor;
    CODE:
        cursor = get_cursor(self);
        if (has_next(self, cursor)) {
          RETVAL = perl_mongo_bson_to_sv(&cursor->buf);
          cursor->at++;

          if (cursor->num == 1 &&
              hv_exists((HV*)SvRV(RETVAL), "$err", strlen("$err"))) {
            STRLEN len;
            SV **err = hv_fetch((HV*)SvRV(RETVAL), "$err", strlen("$err"), 0);
            croak("query error: %s", SvPV_nolen(*err));
          }
	} else {
          RETVAL = &PL_sv_undef;
        }
    OUTPUT:
        RETVAL



SV *
reset (self)
        SV *self
    PREINIT:
        SV *rubbish;
        mongo_cursor *cursor;
    CODE:
        cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);
        cursor->buf.pos = cursor->buf.start;
        cursor->at = 0;
        cursor->num = 0;

	rubbish = perl_mongo_call_method (self, "started_iterating", 1, sv_2mortal(newSViv(0)));
	SvREFCNT_dec(rubbish);



void
DESTROY (self)
      SV *self
  PREINIT:
     mongo_cursor *cursor;
  CODE:
      kill_cursor(self);

      cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);

      if (cursor) {

        if (cursor->buf.start) {
          Safefree(cursor->buf.start);
        }

        Safefree(cursor);
      }

