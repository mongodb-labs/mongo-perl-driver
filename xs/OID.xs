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

static void make_id(char *id) {
  //SV *temp;
  char data[12];

  // the pid is stored in $$
  SV *pid_s = get_sv("$", 0);
  // ...but if it's not, don't crash
  int pid = pid_s ? SvIV(pid_s) : rand();

  // ts increment
  //SV *inc_s = get_sv("MongoDB::OID::_inc", GV_ADD);
  //int inc = SvIV(inc_s);

  int r1 = rand();
  int inc = rand();

  unsigned t = (unsigned) time(0);

  char *T = (char*)&t,
    *M = (char*)&r1,
    *P = (char*)&pid,
    *I = (char*)&inc;

#if MONGO_BIG_ENDIAN
  memcpy(data, T, 4);
  memcpy(data+4, M+1, 3);
  memcpy(data+7, P+2, 2);
  memcpy(data+9, I+1, 3);
#else
  data[0] = T[3];
  data[1] = T[2];
  data[2] = T[1];
  data[3] = T[0];

  memcpy(data+4, M, 3);
  memcpy(data+7, P, 2);
  memcpy(data+9, I, 3);
#endif

  perl_mongo_oid_create(data, id);

  // increment
  //temp = perl_mongo_call_function("MongoDB::OID::_inc", 2, 
  //                                sv_2mortal(newSVpv("MongoDB::OID", 0)), 
  //                                sv_2mortal(newSViv(inc+1)));
  //SvREFCNT_dec(temp);
}


MODULE = MongoDB::OID  PACKAGE = MongoDB::OID

PROTOTYPES: DISABLE

SV *
_build_value (self, c_str)
        SV *self
        const char *c_str;
    PREINIT: 
        char id[25];
    CODE:
        if (c_str && strlen(c_str) == 24) {
          memcpy(id, c_str, 24);
          id[24] = '\0';
        }
        else {
          make_id(id);
        }
        RETVAL = newSVpv (id, 24);
    OUTPUT:
        RETVAL



