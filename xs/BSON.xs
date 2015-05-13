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

MODULE = MongoDB  PACKAGE = MongoDB::BSON

PROTOTYPES: DISABLE

void
_decode_bson(msg, options)
        SV *msg
        SV *options

    PREINIT:
        char * data;
        const bson_t * bson;
        bson_reader_t * reader;
        bool reached_eof;
        STRLEN length;
        HV *opts;

    PPCODE:
        data = SvPV_nolen(msg);
        length = SvCUR(msg);

        if ( options ) {
            if ( SvROK(options) && SvTYPE(SvRV(options)) == SVt_PVHV ) {
                opts = (HV *) SvRV(options);
            }
            else {
                croak("options must be a reference to a hash");
            }
        }

        reader = bson_reader_new_from_data((uint8_t *)data, length);

        while ((bson = bson_reader_read(reader, &reached_eof))) {
          XPUSHs(sv_2mortal(perl_mongo_bson_to_sv(bson, opts)));
        }

        bson_reader_destroy(reader);

void
_encode_bson(doc, options)
        SV *doc
        SV *options
    PREINIT:
        bson_t * bson;
        HV *opts;
    PPCODE:
        bson = bson_new();
        if ( options ) {
            if ( SvROK(options) && SvTYPE(SvRV(options)) == SVt_PVHV ) {
                opts = (HV *) SvRV(options);
            }
            else {
                croak("options must be a reference to a hash");
            }
        }
        perl_mongo_sv_to_bson(bson, doc, opts);
        XPUSHs(sv_2mortal(newSVpvn((const char *)bson_get_data(bson), bson->len)));
        bson_destroy(bson);

SV *
generate_oid ()
    PREINIT:
        bson_oid_t boid;
        char oid[25];
    CODE:
        bson_oid_init(&boid, NULL);
        bson_oid_to_string(&boid, oid);
        RETVAL = newSVpvn(oid, 24);
    OUTPUT:
        RETVAL
