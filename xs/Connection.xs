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
                SV *host_sv = 0, *port_sv = 0, 
                    *left_host_sv = 0, *right_host_sv = 0,
                    *left_port_sv = 0, *right_port_sv = 0,
                    *auto_reconnect_sv = 0, *timeout_sv = 0;
		mongo_link *link;
	INIT:
                New(0, link, 1, mongo_link);
		perl_mongo_attach_ptr_to_instance(self, link);

                host_sv = perl_mongo_call_reader (ST (0), "host");
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

                if (paired) {
                  STRLEN llen, rlen;
                  char *lhost = SvPV(left_host_sv, llen);
                  char *rhost = SvPV(right_host_sv, rlen);
                  
                  link->num = 2;
                  
                  New(0, link->server, link->num, mongo_server*);
                  New(0, link->server[0], 1, mongo_server);
                  New(0, link->server[1], 1, mongo_server);
                  
                  Newz(0, link->server[0]->host, llen+1, char);
                  memcpy(link->server[0]->host, lhost, llen);
                  link->server[0]->port = SvIV(left_port_sv);
                  link->server[0]->connected = 0;
                  
                  Newz(0, link->server[1]->host, rlen+1, char);
                  memcpy(link->server[1]->host, rhost, rlen);
                  link->server[1]->port = SvIV(right_port_sv);
                  link->server[1]->connected = 0;
                }
                else {
                  STRLEN len; 
                  char *host = SvPV(host_sv, len);

                  link->num = 1;
                  
                  New(0, link->server, link->num, mongo_server*);
                  New(0, link->server[0], 1, mongo_server);
                  
                  Newz(0, link->server[0]->host, len+1, char);
                  memcpy(link->server[0]->host, host, len);
                  link->server[0]->port = SvIV(port_sv);
                  link->server[0]->connected = 0;
                }

                auto_reconnect_sv = perl_mongo_call_reader (ST(0), "auto_reconnect");
                timeout_sv = perl_mongo_call_reader (ST(0), "timeout");
	CODE:
                link->master = -1;
                link->ts = time(0);
                link->auto_reconnect = SvIV(auto_reconnect_sv);
                link->timeout = SvIV(timeout_sv);

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
