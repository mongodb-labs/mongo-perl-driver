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
_decode_bson(msg, dt_type, inflate_dbrefs, inflate_regexps, client)
        SV *msg
        char *dt_type
        int inflate_dbrefs
        int inflate_regexps
        SV *client

    PREINIT:
        char * data;
        const bson_t * bson;
        bson_reader_t * reader;
        bool reached_eof;
        STRLEN length;

    PPCODE:
        data = SvPV_nolen(msg);
        length = SvCUR(msg);

        reader = bson_reader_new_from_data((uint8_t *)data, length);

        while ((bson = bson_reader_read(reader, &reached_eof))) {
          XPUSHs(sv_2mortal(perl_mongo_bson_to_sv(bson, dt_type, inflate_dbrefs, inflate_regexps, client)));
        }

        bson_reader_destroy(reader);

void
encode_bson(obj, clean_keys)
         SV *obj
         int clean_keys
    PREINIT:
         bson_t * bson;
    PPCODE:
         bson = bson_new();
         perl_mongo_sv_to_bson(bson, obj, clean_keys, NO_PREP);
         XPUSHs(sv_2mortal(newSVpvn((const char *)bson_get_data(bson), bson->len)));
         bson_destroy(bson);
