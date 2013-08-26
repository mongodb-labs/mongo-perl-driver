/*
 *  Copyright 2009-2013 MongoDB, Inc.
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

static int
cursor_free (pTHX_ SV *sv, MAGIC *mg)
{
    mongo_cursor *cursor;

    PERL_UNUSED_ARG(sv);

    cursor = (mongo_cursor *)mg->mg_ptr;

    if (cursor) {
        if (cursor->buf.start) {
          Safefree(cursor->buf.start);
        }

        Safefree(cursor);
    }

    mg->mg_ptr = NULL;

    return 0;
}

static int
cursor_clone (pTHX_ MAGIC *mg, CLONE_PARAMS *params)
{
    mongo_cursor *cursor, *new_cursor;
    size_t buflen;

    PERL_UNUSED_ARG (params);

    cursor = (mongo_cursor *)mg->mg_ptr;

    Newx(new_cursor, 1, mongo_cursor);
    Copy(cursor, new_cursor, 1, mongo_cursor);

    buflen = cursor->buf.end - cursor->buf.start;
    Newx(new_cursor->buf.start, buflen, char);
    Copy(cursor->buf.start, new_cursor->buf.start, buflen, char);
    new_cursor->buf.end = new_cursor->buf.start + buflen;
    new_cursor->buf.pos =
	new_cursor->buf.start + (cursor->buf.pos - cursor->buf.start);

    mg->mg_ptr = (char *)new_cursor;

    return 0;
}

MGVTBL cursor_vtbl = {
    NULL,
    NULL,
    NULL,
    NULL,
    cursor_free,
#if MGf_COPY
    NULL,
#endif
#if MGf_DUP
    cursor_clone,
#endif
#if MGf_LOCAL
    NULL,
#endif
};

static mongo_cursor* get_cursor(SV *self);
static int has_next(SV *self, mongo_cursor *cursor);
static void kill_cursor(SV *self);

static mongo_cursor* get_cursor(SV *self) {
  perl_mongo_call_method(self, "_do_query", G_DISCARD, 0);
  return (mongo_cursor*)perl_mongo_get_ptr_from_instance(self, &cursor_vtbl);
}

static SV *request_id;

static int has_next(SV *self, mongo_cursor *cursor) {
  SV *link, *limit, *ns, *response_to;
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


  link = perl_mongo_call_reader (self, "_client");
  ns = perl_mongo_call_reader (self, "_ns");

  // we have to go and check with the db
  size = 34+strlen(SvPV_nolen(ns));
  Newx(buf.start, size, char);
  buf.pos = buf.start;
  buf.end = buf.start + size;

  response_to = perl_mongo_call_reader(self, "_request_id");

  CREATE_RESPONSE_HEADER(buf, SvPV_nolen(ns), SvIV(response_to), OP_GET_MORE);

  // change this cursor's request id so we can match the response
  perl_mongo_call_method(self, "_request_id", G_DISCARD, 1, request_id);
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
  mongo_cursor *cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self, &cursor_vtbl);
  SV *link = perl_mongo_call_reader (self, "_client");
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

BOOT:
    request_id = get_sv("MongoDB::Cursor::_request_id", GV_ADD);

void
_init (self)
        SV *self
    PREINIT:
        mongo_cursor *cursor;
    CODE:
        Newxz(cursor, 1, mongo_cursor);

        // attach a mongo_cursor* to the MongoDB::Cursor
        perl_mongo_attach_ptr_to_instance(self, cursor, &cursor_vtbl);



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
        SV *dt_type_sv;
        SV *inflate_dbrefs_sv;
        SV *client_sv;
    CODE:
        cursor = get_cursor(self);
        if (has_next(self, cursor)) {
          dt_type_sv          = perl_mongo_call_reader( self, "_dt_type" );
          inflate_dbrefs_sv   = perl_mongo_call_reader( self, "_inflate_dbrefs" );
          client_sv           = perl_mongo_call_reader( self, "_client" );
          char *dt_type       = SvOK( dt_type_sv ) ? SvPV( dt_type_sv, SvLEN( dt_type_sv ) ) : NULL;
          int  inflate_dbrefs = SvIV( inflate_dbrefs_sv );

          RETVAL = perl_mongo_bson_to_sv( &cursor->buf, dt_type, inflate_dbrefs, client_sv );

          cursor->at++;

          if (cursor->num == 1 &&
              hv_exists((HV*)SvRV(RETVAL), "$err", strlen("$err"))) {
            SV **err = 0, **code = 0;

            err = hv_fetchs((HV*)SvRV(RETVAL), "$err", 0);
            code = hv_fetchs((HV*)SvRV(RETVAL), "code", 0);
            
            if (code && SvIOK(*code) &&
                (SvIV(*code) == 10107 || SvIV(*code) == 13435 || SvIV(*code) == 13436)) {
              SV *conn = perl_mongo_call_method (self, "_client", 0, 0);
              set_disconnected(conn);
            }
            
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
        mongo_cursor *cursor;
    CODE:
        cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self, &cursor_vtbl);
        cursor->buf.pos = cursor->buf.start;
        cursor->at = 0;
        cursor->num = 0;

        perl_mongo_call_method (self, "started_iterating", G_DISCARD, 1, &PL_sv_no);

	RETVAL = SvREFCNT_inc(self);
    OUTPUT:
	RETVAL
        

SV *
info (self)
        SV *self
    PREINIT:
        mongo_cursor *cursor;
        HV *hv;
    CODE:
        cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(self, &cursor_vtbl);
        
        hv = newHV();
        hv_stores(hv, "flag", newSViv(cursor->flag));
        hv_stores(hv, "cursor_id", newSViv(cursor->cursor_id));
        hv_stores(hv, "start", newSViv(cursor->start));
        hv_stores(hv, "at", newSViv(cursor->at));
        hv_stores(hv, "num", newSViv(cursor->num));
        
        RETVAL = newRV_noinc((SV*)hv);
    OUTPUT:
        RETVAL
        
        
void
DESTROY (self)
      SV *self
  PREINIT:
      mongo_link *link;
      SV *link_sv;
  CODE:
      link_sv = perl_mongo_call_reader(self, "_client");
      link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_sv, &connection_vtbl);
      // check if cursor is connected
      if (link->master && link->master->connected) {
          kill_cursor(self);
      }
      SvREFCNT_dec(link_sv);
