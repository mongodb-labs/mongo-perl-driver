/*
 *  Copyright 2009 MongoDB, Inc.
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

#ifndef PERL_MONGO_H
#define PERL_MONGO_H

#define PERL_GCC_BRACE_GROUPS_FORBIDDEN

#include <bson.h>
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

#ifdef WIN32
#include <memory.h>
#endif

#include "regcomp.h"

/* not yet provided by ppport.h */
#ifndef HeUTF8
#define HeUTF8(he)  ((HeKLEN(he) == HEf_SVKEY) ? \
                    SvUTF8(HeKEY_sv(he)) :       \
                    (U32)HeKUTF8(he))
#endif

#define PERL_MONGO_CALL_BOOT(name)  perl_mongo_call_xs (aTHX_ name, cv, mark)

/* whether to add an _id field */
#define PREP 1
#define NO_PREP 0

#ifdef WIN32
#ifdef _MSC_VER
typedef __int64 int64_t;
#define inline __inline
#else
#include <stdint.h>
#endif // _MSC_VER
#endif // WIN32
#include <limits.h>

// define regex macros for Perl 5.8
#ifndef RX_PRECOMP
#define RX_PRECOMP(re) ((re)->precomp)
#define RX_PRELEN(re) ((re)->prelen)
#endif

#define SUBTYPE_BINARY_DEPRECATED 2
#define SUBTYPE_BINARY 0

// struct for 
typedef struct _stackette {
  void *ptr;
  struct _stackette *prev;
} stackette;

#define EMPTY_STACK 0

void perl_mongo_init();
SV *perl_mongo_bson_to_sv (const bson_t * bson, char *dt_type, int inflate_dbrefs, int inflate_regexps, SV *client );
void perl_mongo_sv_to_bson (bson_t * bson, SV *sv, int is_insert, AV *ids);

#endif
