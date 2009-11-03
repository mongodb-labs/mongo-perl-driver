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
                  perl_mongo_link_master(self);
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


AV*
_insert (self, ns, object)
        SV *self
        const char *ns
        SV *object
    PREINIT:
        mongo_link *link;
        mongo_msg_header header;
        buffer buf;
        int i;
        AV *a, *ids;
    INIT:
        a = (AV*)SvRV(object);
        ids = newAV();
    CODE:
        CREATE_BUF(INITIAL_BUF_SIZE);
        CREATE_HEADER(buf, ns, OP_INSERT);

        for (i=0; i<=av_len(a); i++) {
          SV **obj = av_fetch(a, i, 0);
          perl_mongo_sv_to_bson(&buf, *obj, ids);
        }
        perl_mongo_serialize_size(buf.start, &buf);

        // sends
        mongo_link_say(self, &buf);
        Safefree(buf.start);

        RETVAL = (AV*)sv_2mortal((SV*)ids);
    OUTPUT:
        RETVAL


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
        CREATE_BUF(INITIAL_BUF_SIZE);
        CREATE_HEADER(buf, ns, OP_DELETE);
        perl_mongo_serialize_int(&buf, (int)(just_one == 1));
        perl_mongo_sv_to_bson(&buf, query, NO_PREP);
        perl_mongo_serialize_size(buf.start, &buf);

        // sends
        mongo_link_say(self, &buf);
        Safefree(buf.start);


void
_update (self, ns, query, object, flags)
        SV *self
        const char *ns
        SV *query
        SV *object
        int flags
    PREINIT:
        mongo_link *link;
        mongo_msg_header header;
        buffer buf;
    CODE:
        CREATE_BUF(INITIAL_BUF_SIZE);
        CREATE_HEADER(buf, ns, OP_UPDATE);
        perl_mongo_serialize_int(&buf, flags);
        perl_mongo_sv_to_bson(&buf, query, NO_PREP);
        perl_mongo_sv_to_bson(&buf, object, NO_PREP);
        perl_mongo_serialize_size(buf.start, &buf);

        // sends
        mongo_link_say(self, &buf);
        Safefree(buf.start);


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
