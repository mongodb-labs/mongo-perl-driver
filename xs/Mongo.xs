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

extern XS(boot_MongoDB__MongoClient);
extern XS(boot_MongoDB__BSON);
extern XS(boot_MongoDB__Cursor);
extern XS(boot_MongoDB__OID);

MODULE = MongoDB  PACKAGE = MongoDB

PROTOTYPES: DISABLE

BOOT:
	PERL_MONGO_CALL_BOOT (boot_MongoDB__MongoClient);
	PERL_MONGO_CALL_BOOT (boot_MongoDB__BSON);
	PERL_MONGO_CALL_BOOT (boot_MongoDB__Cursor);
	PERL_MONGO_CALL_BOOT (boot_MongoDB__OID);
        gv_fetchpv("MongoDB::Cursor::slave_okay",  GV_ADDMULTI, SVt_IV);
        gv_fetchpv("MongoDB::BSON::looks_like_number",  GV_ADDMULTI, SVt_IV);
        gv_fetchpv("MongoDB::BSON::char",  GV_ADDMULTI, SVt_IV);
        gv_fetchpv("MongoDB::BSON::utf8_flag_on",  GV_ADDMULTI, SVt_IV);
        gv_fetchpv("MongoDB::BSON::use_boolean",  GV_ADDMULTI, SVt_IV);
        gv_fetchpv("MongoDB::BSON::use_binary",  GV_ADDMULTI, SVt_IV);
        perl_mongo_init();

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
         SV *request_id = GvSV(gv_fetchpv("MongoDB::Cursor::_request_id",  GV_ADDMULTI, SVt_IV));
         HV *info = newHV();
         SV **heval;
     PPCODE:
         CREATE_BUF(INITIAL_BUF_SIZE);
         CREATE_HEADER_WITH_OPTS(buf, ns, OP_QUERY, opts);

         perl_mongo_serialize_int(&buf, skip);
         perl_mongo_serialize_int(&buf, limit);

         perl_mongo_sv_to_buffer(&buf, query, NO_PREP);

         if (fields && SvROK(fields)) {
           perl_mongo_sv_to_buffer(&buf, fields, NO_PREP);
         }

         perl_mongo_serialize_size(buf.start, &buf);

         /* this must come after CREATE_HEADER_WITH_OPTS because that
          * increments the request_id */
         heval = hv_stores(info, "ns", newSVpv(ns, strlen(ns)));
         heval = hv_stores(info, "opts", newSViv(opts));
         heval = hv_stores(info, "skip", newSViv(skip));
         heval = hv_stores(info, "limit", newSViv(limit));
         heval = hv_stores(info, "request_id", newSVsv(request_id));

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
         SV *request_id = GvSV(gv_fetchpv("MongoDB::Cursor::_request_id",  GV_ADDMULTI, SVt_IV));
         int i;
         AV *ids = 0;
     INIT:
         if (add_ids) {
            ids = newAV();
         }
     PPCODE:
         CREATE_BUF(INITIAL_BUF_SIZE);
         CREATE_HEADER(buf, ns, OP_INSERT);

         for (i=0; i<=av_len(a); i++) {
           SV **obj = av_fetch(a, i, 0);
           perl_mongo_sv_to_buffer(&buf, *obj, ids);
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
         SV *request_id = GvSV(gv_fetchpv("MongoDB::Cursor::_request_id",  GV_ADDMULTI, SVt_IV));
     PPCODE:
         CREATE_BUF(INITIAL_BUF_SIZE);
         CREATE_HEADER(buf, ns, OP_DELETE);
         perl_mongo_serialize_int(&buf, flags);
         perl_mongo_sv_to_buffer(&buf, criteria, NO_PREP);
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
         SV *request_id = GvSV(gv_fetchpv("MongoDB::Cursor::_request_id",  GV_ADDMULTI, SVt_IV));
    PPCODE:
         CREATE_BUF(INITIAL_BUF_SIZE);
         CREATE_HEADER(buf, ns, OP_UPDATE);
         perl_mongo_serialize_int(&buf, flags);
         perl_mongo_sv_to_buffer(&buf, criteria, NO_PREP);
         perl_mongo_sv_to_buffer(&buf, obj, NO_PREP);
         perl_mongo_serialize_size(buf.start, &buf);

         XPUSHs(sv_2mortal(newSVpvn(buf.start, buf.pos-buf.start)));
         Safefree(buf.start);

void
force_double(input)
	SV *input
    CODE:
	if (SvROK(input)) croak("Can't force a reference into a double");
	SvNV(input);
	SvNOK_only(input);

void
force_int(input)
	SV *input
    CODE:
	if (SvROK(input)) croak("Can't force a reference into an int");
	SvIV(input);
	SvIOK_only(input);
