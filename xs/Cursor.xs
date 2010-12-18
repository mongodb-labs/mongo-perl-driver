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

static mongo_cursor* get_cursor(SV *self);
static int has_next(SV *self, mongo_cursor *cursor);
static void kill_cursor(SV *self);

static mongo_cursor* get_cursor(SV *self) {
  SV *rubbish = perl_mongo_call_method(self, "_do_query", 0);
  SvREFCNT_dec(rubbish);
  return (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);
}

static int has_next(SV *self, mongo_cursor *cursor) {
  SV *link, *limit, *ns, *request_id, *response_to, *rubbish;
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

  response_to = perl_mongo_call_reader(self, "_request_id");
  request_id = get_sv("MongoDB::Cursor::_request_id", GV_ADD);

  CREATE_RESPONSE_HEADER(buf, SvPV_nolen(ns), SvIV(response_to), OP_GET_MORE);

  // change this cursor's request id so we can match the response
  rubbish = perl_mongo_call_method(self, "_request_id", 1, request_id);
  SvREFCNT_dec(rubbish);
  SvREFCNT_dec(response_to);

  perl_mongo_serialize_int(&buf, SvIV(limit));
  perl_mongo_serialize_long(&buf, cursor->cursor_id);
  perl_mongo_serialize_size(buf.start, &buf);

  SvREFCNT_dec(limit);
  SvREFCNT_dec(ns);

  // fails if we're out of elems
  if(mongo_link_say(link, &buf) == -1) {
    SvREFCNT_dec(link);
    Safefree(buf.start);
    die("can't get db response, not connected");
    return 0;
  }

  Safefree(buf.start);

  // if we have cursor->at == cursor->num && recv fails,
  // we're probably just out of results
  // mongo_link_hear returns 1 on success, 0 on failure
  heard = mongo_link_hear(self);
  SvREFCNT_dec(link);
  return heard > 0;
}


static void kill_cursor(SV *self) {
  mongo_cursor *cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);
  SV *link = perl_mongo_call_reader (self, "_connection");
  SV *request_id_sv = perl_mongo_call_reader (self, "_request_id");
  char quickbuf[128];
  buffer buf;
  mongo_msg_header header;

  // we allocate a cursor even if no results are returned, but the database will
  // throw an assertion if we try to kill a non-existant cursor non-cursors have 
  // ids of 0
  if (cursor->cursor_id == 0) {
    SvREFCNT_dec(link);
    SvREFCNT_dec(request_id_sv);
    return;
  }
  buf.pos = quickbuf;
  buf.start = buf.pos;
  buf.end = buf.start + 128;

  // std header
  CREATE_MSG_HEADER(SvIV(request_id_sv), 0, OP_KILL_CURSORS);
  SvREFCNT_dec(request_id_sv);
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
            SV **err = hv_fetch((HV*)SvRV(RETVAL), "$err", strlen("$err"), 0);
            croak("query error: %s", SvPV_nolen(*err));
          }
	} else {
          RETVAL = newSV(0);
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

	RETVAL = SvREFCNT_inc(self);
    OUTPUT:
	RETVAL

void
DESTROY (self)
      SV *self
  PREINIT:
      mongo_cursor *cursor;
      mongo_link *link;
      SV *link_sv;
  CODE:
      link_sv = perl_mongo_call_reader(self, "_connection");
      link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_sv);
      // check if cursor is connected
      if (link->master && link->master->connected) {
          kill_cursor(self);
      }
      SvREFCNT_dec(link_sv);

      cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self);

      if (cursor) {

        if (cursor->buf.start) {
          Safefree(cursor->buf.start);
        }

        Safefree(cursor);
      }

