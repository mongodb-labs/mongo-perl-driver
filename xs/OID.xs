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

MODULE = MongoDB::OID  PACKAGE = MongoDB::OID

PROTOTYPES: DISABLE

SV *
_build_value (self, oid_sv=NULL)
        SV *oid_sv
    PREINIT:
        char id[12], oid[25];
    CODE:
        if (oid_sv) {
          if (sv_len(oid_sv) != 24)
            croak("OIDs need to have a length of 24 bytes");

          Copy(oid, SvPVX(oid_sv), 24, char);
          oid[24] = '\0';
        }
        else {
          perl_mongo_make_id(id);
          perl_mongo_make_oid(id, oid);
        }
        RETVAL = newSVpvn(oid, 24);
    OUTPUT:
        RETVAL



