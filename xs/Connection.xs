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

MODULE = MongoDB::Connection  PACKAGE = MongoDB::Connection

PROTOTYPES: DISABLE

void 
_init_conn(self, hosts=0)
    SV *self
    SV *hosts
  PREINIT:
    SV *auto_reconnect_sv = 0, *timeout_sv = 0;
    int i = 0;
    mongo_link *link;
    AV *av;
  CODE:
    New(0, link, 1, mongo_link);
    perl_mongo_attach_ptr_to_instance(self, link);

    /*
     * hosts are of the form:
     * [{host => "host", port => 27017}, ...]
     */
    av = (AV*)SvRV(hosts);
    link->num = av_len(av)+1;
    New(0, link->server, link->num, mongo_server*);

    for (i=0; i<link->num; i++) {
      STRLEN len;
      const char *host;
      int port;
      HV *hv;
      SV **host_sv, **port_sv, **elem = av_fetch(av, i, 0);

      if (!elem) {
        croak("could not extract host");
        return;
      }

      hv = (HV*)SvRV(*elem);

      host_sv = hv_fetch(hv, "host", strlen("host"), 0);
      host = SvPV(*host_sv, len);

      port_sv = hv_fetch(hv, "port", strlen("port"), 0);
      port = (port_sv && SvOK(*port_sv)) ? SvIV(*port_sv) : 27017;

      New(0, link->server[i], 1, mongo_server);
      
      Newz(0, link->server[i]->host, len+1, char);
      memcpy(link->server[i]->host, host, len);
      link->server[i]->port = port;
      link->server[i]->connected = 0;
    }

    auto_reconnect_sv = perl_mongo_call_reader (ST(0), "auto_reconnect");
    timeout_sv = perl_mongo_call_reader (ST(0), "timeout");

    link->auto_reconnect = SvIV(auto_reconnect_sv);
    link->timeout = SvIV(timeout_sv);

    link->master = -1;
    link->ts = time(0);

  CLEANUP:
    SvREFCNT_dec (auto_reconnect_sv);
    SvREFCNT_dec (timeout_sv);
    

void
connect (self)
     SV *self
   PREINIT:
     int i = 0, connected = 0;
     mongo_link *link = (mongo_link*)perl_mongo_get_ptr_from_instance(self);
   CODE:
     for (i = 0; i < link->num; i++) {
       link->server[i]->socket = perl_mongo_connect(link->server[i]->host, link->server[i]->port, link->timeout);
       link->server[i]->connected = (link->server[i]->socket != -1);

       connected |= link->server[i]->connected;
     }

     // try authentication
     if (connected) {
       SV *username, *password;

       username = perl_mongo_call_reader (self, "username");
       password = perl_mongo_call_reader (self, "password");
       
       if (SvPOK(username) && SvPOK(password)) {
         SV *database, *result, **ok;
         
         database = perl_mongo_call_reader (self, "db_name");
         result = perl_mongo_call_method(self, "authenticate", 3, database, username, password);
         if (!result || SvTYPE(result) != SVt_RV) {
           if (result && SvPOK(result)) {
             croak(SvPV_nolen(result));
             return;
           }
           else { 
             sv_dump(result);
             croak("something weird happened with authentication");
             return;
           }
         }
         
         ok = hv_fetch((HV*)SvRV(result), "ok", strlen("ok"), 0);
         if (!ok || 1 != SvIV(*ok)) {
           SvREFCNT_dec(database);
           SvREFCNT_dec(username);
           SvREFCNT_dec(password);

           croak ("couldn't authenticate with server");
           return;
         }

         SvREFCNT_dec(database);
       }

       SvREFCNT_dec(username);
       SvREFCNT_dec(password);
     }
     else {
       croak ("couldn't connect to server");
       return;
     }

     // croaks on failure
     perl_mongo_master(self);


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
