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
        char * data;
        const bson_t * bson;
        bson_reader_t * reader;
        bool reached_eof;
        STRLEN length;

    PPCODE:
        data = SvPV_nolen(sv);
        length = SvCUR(sv);

        reader = bson_reader_new_from_data((uint8_t *)data, length);

        while ((bson = bson_reader_read(reader, &reached_eof))) {
          XPUSHs(sv_2mortal(perl_mongo_bson_to_sv(bson, "DateTime", 1, 1, newSV(0))));
        }

        bson_reader_destroy(reader);

void
encode_bson(obj)
         SV *obj
    PREINIT:
         bson_t * bson;
    PPCODE:
         bson = bson_new();
         perl_mongo_sv_to_bson(bson, obj, NO_PREP);
         XPUSHs(sv_2mortal(newSVpvn((const char *)bson_get_data(bson), bson->len)));
         bson_destroy(bson);
