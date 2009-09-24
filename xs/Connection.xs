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

MODULE = MongoDB::Connection  PACKAGE = MongoDB::Connection

PROTOTYPES: DISABLE

void
connect (self)
                SV *self
	PREINIT:
                int paired;
                SV *host_sv = 0, *port_sv, 
                    *left_host_sv, *right_host_sv,
                    *left_port_sv, *right_port_sv,
                    *auto_reconnect_sv;
		mongo_link *link;
	INIT:
                left_host_sv = perl_mongo_call_reader (ST(0), "left_host");
                right_host_sv = perl_mongo_call_reader (ST(0), "right_host");

                paired = SvOK(left_host_sv) && SvOK(right_host_sv);
                if (paired) {
                  left_port_sv = perl_mongo_call_reader (ST(0), "left_port");
                  right_port_sv = perl_mongo_call_reader (ST(0), "right_port");
                }
                else {
                  host_sv = perl_mongo_call_reader (ST (0), "host");
                  port_sv = perl_mongo_call_reader (ST (0), "port");
                }

                auto_reconnect_sv = perl_mongo_call_reader (ST(0), "auto_reconnect");
	CODE:
	        Newx(link, 1, mongo_link);
		perl_mongo_attach_ptr_to_instance(self, link);

                link->paired = paired;
                link->master = -1;
                link->ts = time(0);
                link->auto_reconnect = SvIV(auto_reconnect_sv);
                if (paired) {
                  int llen = strlen(SvPV_nolen(left_host_sv));
                  int rlen = strlen(SvPV_nolen(right_host_sv));

                  Newxz(link->server.pair.left_host, llen+1, char);
                  memcpy(link->server.pair.left_host, SvPV_nolen(left_host_sv), llen);
                  link->server.pair.left_port = SvIV(left_port_sv);
                  link->server.pair.left_connected = 0;

                  Newxz(link->server.pair.right_host, rlen+1, char);
                  memcpy(link->server.pair.right_host, SvPV_nolen(right_host_sv), rlen);
                  link->server.pair.right_port = SvIV(right_port_sv);
                  link->server.pair.right_connected = 0;
                }
                else { 
                  int len = strlen(SvPV_nolen(host_sv));
                  Newxz(link->server.single.host, len+1, char);
                  memcpy(link->server.single.host, SvPV_nolen(host_sv), len);
                  link->server.single.port = SvIV(port_sv);
                  link->server.single.connected = 0;
                }

                if (!mongo_link_connect(link)) {
                  croak ("couldn't connect to server");
                  return;
		}

                if (paired) {
                  perl_mongo_link_master(self, link);
                }
	CLEANUP:
                if (paired) {
                  SvREFCNT_dec(left_host_sv);
                  SvREFCNT_dec(left_port_sv);
                  SvREFCNT_dec(right_host_sv);
                  SvREFCNT_dec(right_port_sv);
                }
                else {
                  SvREFCNT_dec (host_sv);
                  SvREFCNT_dec (port_sv);
                }
                SvREFCNT_dec (auto_reconnect_sv);


SV *
_query (self, ns, query=0, limit=0, skip=0, sort=0)
        SV *self
        const char *ns
        SV *query
        int limit
        int skip
        SV *sort
    PREINIT:
        mongo_cursor *cursor;
        SV **socket;
        HV *this_hash, *stash, *rcursor, *full_query;
    CODE:
        // create a new MongoDB::Cursor
        stash = gv_stashpv("MongoDB::Cursor", 0);
        rcursor = newHV();
        RETVAL = sv_bless(newRV_noinc((SV *)rcursor), stash);

        // associate this connection with the cursor
        //SvREFCNT_inc(SvRV(self));
        hv_store(stash, "link", strlen("link"), newRV_inc(SvRV(self)), 0);

        // attach a mongo_cursor* to the MongoDB::Cursor
        Newx(cursor, 1, mongo_cursor);
        perl_mongo_attach_ptr_to_instance(RETVAL, cursor);

        // START cursor setup

        // set the namespace
        Newxz(cursor->ns, strlen(ns)+1, char);
        memcpy(cursor->ns, (char*)ns, strlen(ns));

        // create the query
        full_query = newHV();
        cursor->query = newRV((SV*)full_query);

        // add the query to the... query
        if (!query || !SvOK(query)) {
          query = newRV_noinc((SV*)newHV());
        }
        SvREFCNT_inc(query);
        hv_store(full_query, "query", strlen("query"), query, 0);

        // add sort to the query
        if (sort && SvOK(sort)) {
          hv_store(full_query, "orderby", strlen("orderby"), SvREFCNT_inc(sort), 0);
        }
        hv_store(stash, "query", strlen("query"), newRV_noinc((SV*)full_query), 0);

        // add limit/skip
        cursor->limit = limit;
        cursor->skip = skip;

	// zero results fields
	cursor->num = 0;
	cursor->at = 0;

        // zero other fields
        cursor->fields = 0;
        cursor->opts = 0;
        cursor->started_iterating = 0;

        // clear the buf
        cursor->buf.start = 0;
        cursor->buf.pos = 0;
        cursor->buf.end = 0;

        // STOP cursor setup

    OUTPUT:
        RETVAL


SV *
_find_one (self, ns, query)
	SV *self
        const char *ns
        SV *query
    PREINIT:
        SV *cursor;
    CODE:
        // create a cursor with limit = -1
        cursor = perl_mongo_call_method(self, "_query", 3, ST(1), ST(2), newSViv(-1));
        RETVAL = perl_mongo_call_method(cursor, "next", 0);
    OUTPUT:
        RETVAL
    CLEANUP:
        SvREFCNT_dec (cursor);


void
_insert (self, ns, object)
        SV *self
        const char *ns
        SV *object
    PREINIT:
        mongo_link *link;
        mongo_msg_header header;
        buffer buf;
        int i;
        AV *a;
    INIT:
        a = (AV*)SvRV(object);
    CODE:
        link = (mongo_link*)perl_mongo_get_ptr_from_instance(self);

        CREATE_BUF(INITIAL_BUF_SIZE);
        CREATE_HEADER(buf, ns, OP_INSERT);

        for (i=0; i<=av_len(a); i++) {
          SV **obj = av_fetch(a, i, 0);
          perl_mongo_sv_to_bson(&buf, *obj, PREP);
        }
        perl_mongo_serialize_size(buf.start, &buf);

        // sends
        mongo_link_say(self, link, &buf);
        Safefree(buf.start);


void
_remove (self, ns, query, just_one)
        SV *self
        const char *ns
        SV *query
        bool just_one
    PREINIT:
        mongo_link *link;
        mongo_msg_header header;
        buffer buf;
    CODE:
        link = (mongo_link*)perl_mongo_get_ptr_from_instance(self);

        CREATE_BUF(INITIAL_BUF_SIZE);
        CREATE_HEADER(buf, ns, OP_DELETE);
        perl_mongo_serialize_int(&buf, (int)(just_one == 1));
        perl_mongo_sv_to_bson(&buf, query, NO_PREP);
        perl_mongo_serialize_size(buf.start, &buf);

        // sends
        mongo_link_say(self, link, &buf);
        Safefree(buf.start);


void
_update (self, ns, query, object, upsert)
        SV *self
        const char *ns
        SV *query
        SV *object
        bool upsert
    PREINIT:
        mongo_link *link;
        mongo_msg_header header;
        buffer buf;
    CODE:
        link = (mongo_link*)perl_mongo_get_ptr_from_instance(self);

        CREATE_BUF(INITIAL_BUF_SIZE);
        CREATE_HEADER(buf, ns, OP_UPDATE);
        perl_mongo_serialize_int(&buf, upsert);
        perl_mongo_sv_to_bson(&buf, query, NO_PREP);
        perl_mongo_sv_to_bson(&buf, object, NO_PREP);
        perl_mongo_serialize_size(buf.start, &buf);

        // sends
        mongo_link_say(self, link, &buf);
        Safefree(buf.start);

void
_ensure_index (self, ns, keys, unique=0)
	SV *self
        const char *ns
        SV *keys
        int unique
    PREINIT:
        AV *key_array;
        SV **key_hash;
        SV *ret;
    INIT:
        key_array = (AV*)SvRV(keys);
        key_hash = av_fetch(key_array, 0, 0);
    CODE:
        hv_store((HV*)SvRV(*key_hash), "unique", strlen("unique"), unique ? &PL_sv_yes : &PL_sv_no, 0);
        ret = perl_mongo_call_method(self, "_insert", 2, ST(1), ST(2));
    CLEANUP:
        SvREFCNT_dec (ret);


void
DESTROY (self)
          SV *self
     PREINIT:
         mongo_link *link;
     CODE:
         link = (mongo_link*)perl_mongo_get_ptr_from_instance(self);
         if (link->paired) {
           Safefree(link->server.pair.left_host);
           Safefree(link->server.pair.right_host);
         }
         else {
           Safefree(link->server.single.host);
         }
         Safefree(link);
