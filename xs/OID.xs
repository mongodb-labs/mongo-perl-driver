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

MODULE = MongoDB::OID  PACKAGE = MongoDB::OID

PROTOTYPES: DISABLE

SV *
_build_value (self, c_str)
        SV *self
        const char *c_str;
    PREINIT: 
        char *id;
    INIT:
        Newxz(id, 25, char);
    CODE:
        if (c_str && strlen(c_str) == 24) {
          memcpy(id, c_str, 24);
        }
        else {
          int i;
          char *movable, *id_str, *T;
          char data[12];
          unsigned t;

          int r1 = rand();
          int r2 = rand();
          
          char *inc = (char*)(void*)&r2;
          t = (unsigned) time(0);

          T = (char*)&t;
          data[0] = T[3];
          data[1] = T[2];
          data[2] = T[1];
          data[3] = T[0];

          memcpy(data+4, &r1, 4);
          data[8] = inc[3];
          data[9] = inc[2];
          data[10] = inc[1];
          data[11] = inc[0];

          perl_mongo_oid_create(data, id);
        }
        RETVAL = newSVpv (id, 24);
    OUTPUT:
        RETVAL



