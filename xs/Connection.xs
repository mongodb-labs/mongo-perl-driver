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

MGVTBL connection_vtbl;

MODULE = MongoDB::Connection  PACKAGE = MongoDB::Connection

PROTOTYPES: DISABLE

void 
_init_conn(self, host, port)
    SV *self
    char *host
    int port
  PREINIT:
    SV *auto_reconnect_sv = 0, *timeout_sv = 0;
    mongo_link *link;
  CODE:
    New(0, link, 1, mongo_link);
    perl_mongo_attach_ptr_to_instance(self, link, &connection_vtbl);

    /*
     * hosts are of the form:
     * [{host => "host", port => 27017}, ...]
     */
    New(0, link->master, 1, mongo_server);      
    Newz(0, link->master->host, strlen(host)+1, char);
    memcpy(link->master->host, host, strlen(host));
    link->master->port = port;
    link->master->connected = 0;

    auto_reconnect_sv = perl_mongo_call_reader (ST(0), "auto_reconnect");
    timeout_sv = perl_mongo_call_reader (ST(0), "timeout");

    link->auto_reconnect = SvIV(auto_reconnect_sv);
    link->timeout = SvIV(timeout_sv);
    link->copy = 0;

  CLEANUP:
    SvREFCNT_dec (auto_reconnect_sv);
    SvREFCNT_dec (timeout_sv);

void 
_init_conn_holder(self, master)
    SV *self
    SV *master
  PREINIT:
    mongo_link *self_link, *master_link;
  CODE:
    New(0, self_link, 1, mongo_link);
    perl_mongo_attach_ptr_to_instance(self, self_link, &connection_vtbl);

    master_link = (mongo_link*)perl_mongo_get_ptr_from_instance(master, &connection_vtbl);

    self_link->master = master_link->master;
    self_link->copy = 1;
    

void
connect (self)
     SV *self
   PREINIT:
     mongo_link *link = (mongo_link*)perl_mongo_get_ptr_from_instance(self, &connection_vtbl);
     SV *username, *password;
   CODE:
     link->master->socket = perl_mongo_connect(link->master->host, link->master->port, link->timeout);
     link->master->connected = link->master->socket != -1;

     if (!link->master->connected) {
       croak ("couldn't connect to server %s:%d", link->master->host, link->master->port);
     }

     // try authentication
     username = perl_mongo_call_reader (self, "username");
     password = perl_mongo_call_reader (self, "password");

     if (SvPOK(username) && SvPOK(password)) {
       SV *database, *result, **ok;
         
       database = perl_mongo_call_reader (self, "db_name");
       result = perl_mongo_call_method(self, "authenticate", 3, database, username, password);
       if (!result || SvTYPE(result) != SVt_RV) {
         if (result && SvPOK(result)) {
           croak("%s", SvPV_nolen(result));
         }
         else { 
           sv_dump(result);
           croak("something weird happened with authentication");
         }
       }
         
       ok = hv_fetch((HV*)SvRV(result), "ok", strlen("ok"), 0);
       if (!ok || 1 != SvIV(*ok)) {
         SvREFCNT_dec(database);
         SvREFCNT_dec(username);
         SvREFCNT_dec(password);

         croak ("couldn't authenticate with server");
       }

       SvREFCNT_dec(database);
     }

     SvREFCNT_dec(username);
     SvREFCNT_dec(password);


int
connected(self)
     SV *self
  INIT:
     mongo_link *link;
  CODE:
     link = (mongo_link*)perl_mongo_get_ptr_from_instance(self, &connection_vtbl);

     if (link->master && link->master->connected) {
         RETVAL = 1;
     }
     else {
         RETVAL = 0;
     }
  OUTPUT:
     RETVAL


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
         SV *cursor
     CODE:
         mongo_link_hear(cursor);


void
DESTROY (self)
          SV *self
     PREINIT:
         mongo_link *link;
     CODE:
         link = (mongo_link*)perl_mongo_get_ptr_from_instance(self, &connection_vtbl);

         if (!link->copy && link->master) {
           set_disconnected(self);

           if (link->master->host) {
             Safefree(link->master->host);
           }

           Safefree(link->master);
         }

         Safefree(link);
