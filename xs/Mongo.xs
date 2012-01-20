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

extern XS(boot_MongoDB__Connection);
extern XS(boot_MongoDB__BSON);
extern XS(boot_MongoDB__Cursor);
extern XS(boot_MongoDB__OID);

MODULE = MongoDB  PACKAGE = MongoDB

PROTOTYPES: DISABLE

BOOT:
        if (items < 3)
            croak("machine id required");

        perl_mongo_machine_id = SvIV(ST(2));

	PERL_MONGO_CALL_BOOT (boot_MongoDB__Connection);
	PERL_MONGO_CALL_BOOT (boot_MongoDB__BSON);
	PERL_MONGO_CALL_BOOT (boot_MongoDB__Cursor);
	PERL_MONGO_CALL_BOOT (boot_MongoDB__OID);
        gv_fetchpv("MongoDB::Cursor::_request_id",  GV_ADDMULTI, SVt_IV);
        gv_fetchpv("MongoDB::Cursor::slave_okay",  GV_ADDMULTI, SVt_IV);
        gv_fetchpv("MongoDB::BSON::looks_like_number",  GV_ADDMULTI, SVt_IV);
        gv_fetchpv("MongoDB::BSON::char",  GV_ADDMULTI, SVt_IV);
        gv_fetchpv("MongoDB::BSON::utf8_flag_on",  GV_ADDMULTI, SVt_IV);
        gv_fetchpv("MongoDB::BSON::use_boolean",  GV_ADDMULTI, SVt_IV);
        perl_mongo_mutex_init();

void
write_query(ns, opts, skip, limit, query, fields = 0)
         char *ns
         int opts
         int skip
         int limit
         SV *query
         SV *fields
     PREINIT:
         buffer buf;
         mongo_msg_header header;
         HV *info = newHV();
         SV **heval, *request_id;
     PPCODE:
         request_id = get_sv("MongoDB::Cursor::_request_id", GV_ADD);
         heval = hv_store(info, "ns", strlen("ns"), newSVpv(ns, strlen(ns)), 0);
         heval = hv_store(info, "opts", strlen("opts"), newSViv(opts), 0);
         heval = hv_store(info, "skip", strlen("skip"), newSViv(skip), 0);
         heval = hv_store(info, "limit", strlen("limit"), newSViv(limit), 0);
         heval = hv_store(info, "request_id", strlen("request_id"), SvREFCNT_inc(request_id), 0);

         CREATE_BUF(INITIAL_BUF_SIZE);
         CREATE_HEADER_WITH_OPTS(buf, ns, OP_QUERY, opts);

         perl_mongo_serialize_int(&buf, skip);
         perl_mongo_serialize_int(&buf, limit);

         perl_mongo_sv_to_bson(&buf, query, NO_PREP);

         if (fields && SvROK(fields)) {
           perl_mongo_sv_to_bson(&buf, fields, NO_PREP);
         }

         perl_mongo_serialize_size(buf.start, &buf);

         XPUSHs(sv_2mortal(newSVpvn(buf.start, buf.pos-buf.start)));
         XPUSHs(sv_2mortal(newRV_noinc((SV*)info)));

         Safefree(buf.start);


void
write_insert(ns, a, add_ids)
         char *ns
         AV *a
         int add_ids
     PREINIT:
         buffer buf;
         mongo_msg_header header;
         int i;
         AV *ids = 0;
         SV *request_id;
     INIT:
         if (add_ids) {
            ids = newAV();
         }
     PPCODE:
         request_id = get_sv("MongoDB::Cursor::_request_id", GV_ADD);

         CREATE_BUF(INITIAL_BUF_SIZE);
         CREATE_HEADER(buf, ns, OP_INSERT);

         for (i=0; i<=av_len(a); i++) {
           int start = buf.pos-buf.start;
           SV **obj = av_fetch(a, i, 0);
           perl_mongo_sv_to_bson(&buf, *obj, ids);
         }
         perl_mongo_serialize_size(buf.start, &buf);

         XPUSHs(sv_2mortal(newSVpvn(buf.start, buf.pos-buf.start)));
         if (add_ids) {
           XPUSHs(sv_2mortal(newRV_noinc((SV*)ids)));
         }

         Safefree(buf.start);

void
write_remove(ns, criteria, flags)
         char *ns
         SV *criteria
         int flags
     PREINIT:
         buffer buf;
         mongo_msg_header header;
         SV *request_id;
     PPCODE:
         request_id = get_sv("MongoDB::Cursor::_request_id", GV_ADD);

         CREATE_BUF(INITIAL_BUF_SIZE);
         CREATE_HEADER(buf, ns, OP_DELETE);
         perl_mongo_serialize_int(&buf, flags);
         perl_mongo_sv_to_bson(&buf, criteria, NO_PREP);
         perl_mongo_serialize_size(buf.start, &buf);

         XPUSHs(sv_2mortal(newSVpvn(buf.start, buf.pos-buf.start)));
         Safefree(buf.start);

void
write_update(ns, criteria, obj, flags)
         char *ns
         SV *criteria
         SV *obj
         int flags
    PREINIT:
         buffer buf;
         mongo_msg_header header;
         SV *request_id;
    PPCODE:
         request_id = get_sv("MongoDB::Cursor::_request_id", GV_ADD);

         CREATE_BUF(INITIAL_BUF_SIZE);
         CREATE_HEADER(buf, ns, OP_UPDATE);
         perl_mongo_serialize_int(&buf, flags);
         perl_mongo_sv_to_bson(&buf, criteria, NO_PREP);
         perl_mongo_sv_to_bson(&buf, obj, NO_PREP);
         perl_mongo_serialize_size(buf.start, &buf);

         XPUSHs(sv_2mortal(newSVpvn(buf.start, buf.pos-buf.start)));
         Safefree(buf.start);

void
read_documents(sv)
         SV *sv
    PREINIT:
         buffer buf;
    PPCODE:
         buf.start = SvPV_nolen(sv);
         buf.pos = buf.start;
         buf.end = buf.start + SvCUR(sv);

         while(buf.pos < buf.end) {
             XPUSHs(sv_2mortal(perl_mongo_bson_to_sv(&buf)));
         }

