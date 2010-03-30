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
connect (self, hosts=0)
                SV *self
                SV *hosts
	PREINIT:
                SV *auto_reconnect_sv = 0, *timeout_sv = 0;
	        mongo_link *link;
                HV *hv;
                HE *he;
	INIT:
                New(0, link, 1, mongo_link);
		perl_mongo_attach_ptr_to_instance(self, link);

                hv = (HV*)SvRV(hosts);
                link->num = HvKEYS(hv);
                New(0, link->server, link->num, mongo_server*);

                (void)hv_iterinit (hv);
                while ((he = hv_iternext (hv))) {
                  STRLEN len;
                  const char *host = HePV (he, len);
                  SV **hval = hv_fetch(hv, host, len, 0);
                  int port = (hval && SvOK(*hval)) ? SvIV(*hval) : 27017;

                  New(0, link->server[0], 1, mongo_server);
                  
                  Newz(0, link->server[0]->host, len+1, char);
                  memcpy(link->server[0]->host, host, len);
                  link->server[0]->port = port;
                  link->server[0]->connected = 0;
                }

                auto_reconnect_sv = perl_mongo_call_reader (ST(0), "auto_reconnect");
                timeout_sv = perl_mongo_call_reader (ST(0), "timeout");

                link->auto_reconnect = SvIV(auto_reconnect_sv);
                link->timeout = SvIV(timeout_sv);

                link->master = -1;
                link->ts = time(0);
	CODE:

                if (!mongo_link_connect(link)) {
                  croak ("couldn't connect to server");
                  return;
		}

                perl_mongo_link_master(self);
	CLEANUP:
                SvREFCNT_dec (auto_reconnect_sv);
                SvREFCNT_dec (timeout_sv);


int
send(self, str)
         SV *self
         SV *str
     PREINIT:
         buffer buf;
         STRLEN len;
     INIT:
         buf.start = SvPV(str,len);
         buf.pos = buf.start+len;
         buf.end = buf.start+len;
     CODE:
         RETVAL = mongo_link_say(self, &buf);
         if (RETVAL == -1) {
           die("can't get db response, not connected");
         }
     OUTPUT:
         RETVAL


void
recv(self, cursor)
         SV *self
         SV *cursor
     CODE:
         mongo_link_hear(cursor);


void
DESTROY (self)
          SV *self
     PREINIT:
         mongo_link *link;
         int i = 0;
     CODE:
         link = (mongo_link*)perl_mongo_get_ptr_from_instance(self);

         for (i = 0; i < link->num; i++) {
           if (link->server[i]->connected) {
#ifdef WIN32
             closesocket(link->server[i]->socket);
#else
             close(link->server[i]->socket);
#endif
           }

           if (link->server[i]->host) {
             Safefree(link->server[i]->host);
           }

           Safefree(link->server[i]);
         }

         Safefree(link->server);
         Safefree(link);
