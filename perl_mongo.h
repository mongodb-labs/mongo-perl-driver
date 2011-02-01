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

#ifndef PERL_MONGO_H
#define PERL_MONGO_H

#define PERL_GCC_BRACE_GROUPS_FORBIDDEN

#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"

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

// define regex macros for Perl 5.8
#ifndef RX_PRECOMP
#define RX_PRECOMP(re) ((re)->precomp)
#define RX_PRELEN(re) ((re)->prelen)
#endif

#if MONGO_BIG_ENDIAN

#define BYTE1_32(b) ((b & 0xff000000) >> 24)
#define BYTE2_32(b) ((b & 0x00ff0000) >> 8)
#define BYTE3_32(b) ((b & 0x0000ff00) << 8)
#define BYTE4_32(b) ((b & 0x000000ff) << 24)

#define MONGO_32(b) (BYTE4_32(b) | BYTE3_32(b) | BYTE2_32(b) | BYTE1_32(b))

#define MONGO_32p(b) (((int)((unsigned char)b[0])) | ((int)((unsigned char)b[1]) << 8) | \
                      ((int)((unsigned char)b[2]) << 16) | ((int)((unsigned char)b[3]) << 24))

#define BYTE1_64(b) ((b & 0xff00000000000000ll) >> 56)
#define BYTE2_64(b) ((b & 0x00ff000000000000ll) >> 40)
#define BYTE3_64(b) ((b & 0x0000ff0000000000ll) >> 24)
#define BYTE4_64(b) ((b & 0x000000ff00000000ll) >> 8)
#define BYTE5_64(b) ((b & 0x00000000ff000000ll) << 8)
#define BYTE6_64(b) ((b & 0x0000000000ff0000ll) << 24)
#define BYTE7_64(b) ((b & 0x000000000000ff00ll) << 40)
#define BYTE8_64(b) ((b & 0x00000000000000ffll) << 56)

#define MONGO_64(b) (BYTE8_64(b) | BYTE7_64(b) | BYTE6_64(b) | BYTE5_64(b) | \
                     BYTE4_64(b) | BYTE3_64(b) | BYTE2_64(b) | BYTE1_64(b))

#define MONGO_64p(b) (((int64_t)((unsigned char)b[0])) | ((int64_t)((unsigned char)b[1]) << 8) | \
                      ((int64_t)((unsigned char)b[2]) << 16) | ((int64_t)((unsigned char)b[3]) << 24) | \
                      ((int64_t)((unsigned char)b[4]) << 32) | ((int64_t)((unsigned char)b[5]) << 40) | \
                      ((int64_t)((unsigned char)b[6]) << 48) | ((int64_t)((unsigned char)b[7]) << 56))

#else
#define MONGO_32(b) (b)
#define MONGO_32p(b) *((int*)(b))

#define MONGO_64(b) (b)
#define MONGO_64p(b) *((int64_t*)(b))
#endif



#define INT_32 4
#define INT_64 8
#define DOUBLE_64 8
#define BYTE_8 1
#define OID_SIZE 12

#define BSON_DOUBLE 1
#define BSON_STRING 2
#define BSON_OBJECT 3
#define BSON_ARRAY 4
#define BSON_BINARY 5
#define BSON_UNDEF 6
#define BSON_OID 7
#define BSON_BOOL 8
#define BSON_DATE 9
#define BSON_NULL 10
#define BSON_REGEX 11
#define BSON_DBREF 12
#define BSON_CODE__D 13
#define BSON_SYMBOL 14
#define BSON_CODE 15
#define BSON_INT 16
#define BSON_TIMESTAMP 17
#define BSON_LONG 18
#define BSON_MINKEY -1
#define BSON_MAXKEY 127

#define GROW_SLOWLY 1048576
#define MAX_OBJ_SIZE 1024*1024*4

typedef struct {
  char *start;
  char *pos;
  char *end;
} buffer;

// struct for 
typedef struct _stackette {
  void *ptr;
  struct _stackette *prev;
} stackette;

#define EMPTY_STACK 0

// it's safer to leave this signed in case there are any other missing BUF_REMAININGs
#define BUF_REMAINING (buf->end-buf->pos)
#define set_type(buf, type) perl_mongo_serialize_byte(buf, (char)type)
#define perl_mongo_serialize_null(buf) perl_mongo_serialize_byte(buf, (char)0)
#define perl_mongo_serialize_bool(buf, b) perl_mongo_serialize_byte(buf, (char)b)

extern MGVTBL connection_vtbl, cursor_vtbl;
extern int perl_mongo_machine_id;

void perl_mongo_mutex_init();
void perl_mongo_call_xs (pTHX_ void (*subaddr) (pTHX_ CV *cv), CV *cv, SV **mark);
SV *perl_mongo_call_reader (SV *self, const char *reader);
SV *perl_mongo_call_method (SV *self, const char *method, I32 flags, int num, ...);
SV *perl_mongo_call_function (const char *func, int num, ...);
void perl_mongo_attach_ptr_to_instance (SV *self, void *ptr, MGVTBL *vtbl);
void *perl_mongo_get_ptr_from_instance (SV *self, MGVTBL *vtbl);
SV *perl_mongo_construct_instance (const char *klass, ...);
SV *perl_mongo_construct_instance_va (const char *klass, va_list ap);
SV *perl_mongo_construct_instance_with_magic (const char *klass, void *ptr, MGVTBL *vtbl, ...);

void perl_mongo_make_id(char *id);
void perl_mongo_make_oid(char* twelve, char *twenty4);

// serialization
SV *perl_mongo_bson_to_sv (buffer *buf);
void perl_mongo_sv_to_bson (buffer *buf, SV *sv, AV *ids);

int perl_mongo_resize_buf (buffer*, int);
void perl_mongo_serialize_key(buffer *buf, const char *str, int is_insert);
void perl_mongo_serialize_size(char*, buffer*);
void perl_mongo_serialize_double(buffer*, double);
void perl_mongo_serialize_string(buffer*, const char*, unsigned int);
void perl_mongo_serialize_long(buffer*, int64_t);
void perl_mongo_serialize_int(buffer*, int);
void perl_mongo_serialize_byte(buffer*, char);
void perl_mongo_serialize_bytes(buffer*, const char*, unsigned int);

#endif
