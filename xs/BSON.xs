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

MODULE = MongoDB::BSON  PACKAGE = MongoDB::BSON

PROTOTYPES: DISABLE

void
decode_bson(sv)
         SV *sv
    PREINIT:
         buffer buf;
    PPCODE:
         buf.start = SvPV_nolen(sv);
         buf.pos = buf.start;
         buf.end = buf.start + SvCUR(sv);

         while(buf.pos < buf.end) {
           XPUSHs(sv_2mortal(perl_mongo_bson_to_sv(&buf, "DateTime", 1, newSV(0) )));
         }

void
encode_bson(obj)
         SV *obj
    PREINIT:
         buffer buf;
    PPCODE:
         CREATE_BUF(INITIAL_BUF_SIZE);
         perl_mongo_sv_to_bson(&buf, obj, NO_PREP);
         perl_mongo_serialize_size(buf.start, &buf);
         XPUSHs(sv_2mortal(newSVpvn(buf.start, buf.pos-buf.start)));
         Safefree(buf.start);

