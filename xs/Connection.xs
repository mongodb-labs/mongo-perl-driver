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

static int
connection_free (pTHX_ SV *sv, MAGIC *mg)
{
    mongo_link *link;

    PERL_UNUSED_ARG(sv);

    link = (mongo_link *)mg->mg_ptr;

    if (!link->copy && link->master) {
        if (link->master->host) {
            Safefree(link->master->host);
        }

        Safefree(link->master);
    }

    Safefree(link);

    mg->mg_ptr = NULL;

    return 0;
}

static int
connection_clone (pTHX_ MAGIC *mg, CLONE_PARAMS *params)
{
    mongo_link *link, *new_link;

    PERL_UNUSED_ARG (params);

    link = (mongo_link *)mg->mg_ptr;

    Newx(new_link, 1, mongo_link);
    Copy(link, new_link, 1, mongo_link);

    if (link->master) {
        mongo_server *new_master;

        Newx(new_master, 1, mongo_server);
        new_master->host = savepv(link->master->host);
        new_master->port = link->master->port;

        /* Start out disconnected. When we have something to send, we'll
         * reconnect automatically.
         *
         * If we actually wanted to reconnect here, we'd have to make mongo_link
         * carry around a backref to the SV it's associated with so we could
         * reconnect through perl space.
         */
        new_master->connected = 0;

        new_link->master = new_master;
    }


    mg->mg_ptr = (char *)new_link;

    return 0;
}

MGVTBL connection_vtbl = {
    NULL,
    NULL,
    NULL,
    NULL,
    connection_free,
#if MGf_COPY
    NULL,
#endif
#if MGf_DUP
    connection_clone,
#endif
#if MGf_LOCAL
    NULL,
#endif
};

MODULE = MongoDB::Connection  PACKAGE = MongoDB::Connection

PROTOTYPES: DISABLE

void 
_init_conn(self, host, port, ssl)
    SV *self
    char *host
    int port
    bool ssl
  PREINIT:
    SV *auto_reconnect_sv = 0, *timeout_sv = 0;
    mongo_link *link;
  CODE:
    Newx(link, 1, mongo_link);
    perl_mongo_attach_ptr_to_instance(self, link, &connection_vtbl);

    /*
     * hosts are of the form:
     * [{host => "host", port => 27017}, ...]
     */
    Newx(link->master, 1, mongo_server);
    Newxz(link->master->host, strlen(host)+1, char);
    memcpy(link->master->host, host, strlen(host));
    link->master->port = port;
    link->master->connected = 0;
    link->ssl = ssl;
    link->ssl_handle = NULL;
    link->ssl_context = NULL;

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
    Newx(self_link, 1, mongo_link);
    perl_mongo_attach_ptr_to_instance(self, self_link, &connection_vtbl);

    master_link = (mongo_link*)perl_mongo_get_ptr_from_instance(master, &connection_vtbl);

    self_link->master = master_link->master;
    self_link->copy = 1;
    self_link->ssl = master_link->ssl;
    self_link->ssl_handle = master_link->ssl_handle;
    self_link->ssl_context = master_link->ssl_context;
    self_link->send = master_link->send;
    self_link->recv = master_link->recv;    

void
connect (self)
     SV *self
   PREINIT:
     mongo_link *link = (mongo_link*)perl_mongo_get_ptr_from_instance(self, &connection_vtbl);
     SV *username, *password;
   CODE:
    perl_mongo_connect(link);

     if (!link->master->connected) {
       croak ("couldn't connect to server %s:%d", link->master->host, link->master->port);
     }

     // try authentication
     username = perl_mongo_call_reader (self, "username");
     password = perl_mongo_call_reader (self, "password");

     if (SvPOK(username) && SvPOK(password)) {
       SV *database, *result, **ok;
         
       database = perl_mongo_call_reader (self, "db_name");
       result = perl_mongo_call_method(self, "authenticate", 0, 3, database, username, password);
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
         }
