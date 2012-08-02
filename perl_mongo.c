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
#include "mongo_link.h"

#ifdef WIN32
#include <memory.h>
#endif

#include "regcomp.h"

static stackette* check_circular_ref(void *ptr, stackette *stack);
static int isUTF8(const char*, int);
static void serialize_regex(buffer*, const char*, REGEXP*, int is_insert);
static void serialize_regex_flags(buffer*, SV*);
static void append_sv (buffer *buf, const char *key, SV *sv, stackette *stack, int is_insert);

#ifdef USE_ITHREADS
static perl_mutex inc_mutex;
#endif

static int perl_mongo_inc = 0;
int perl_mongo_machine_id;

void perl_mongo_mutex_init() {
  MUTEX_INIT(&inc_mutex);
}

void
perl_mongo_call_xs (pTHX_ void (*subaddr) (pTHX_ CV *), CV *cv, SV **mark) {
  dSP;
  PUSHMARK (mark);
  (*subaddr) (aTHX_ cv);
  PUTBACK;
}

SV *
perl_mongo_call_reader (SV *self, const char *reader) {
  dSP;
  SV *ret;
  I32 count;

  ENTER;
  SAVETMPS;

  PUSHMARK (SP);
  XPUSHs (self);
  PUTBACK;

  count = call_method (reader, G_SCALAR);

  SPAGAIN;

  if (count != 1) {
    croak ("reader didn't return a value");
  }

  ret = POPs;
  SvREFCNT_inc (ret);

  PUTBACK;
  FREETMPS;
  LEAVE;

  return ret;
}


SV *
perl_mongo_call_method (SV *self, const char *method, I32 flags, int num, ...) {
  dSP;
  SV *ret = NULL;
  I32 count;
  va_list args;

  if (flags & G_ARRAY) {
    croak("perl_mongo_call_method doesn't support list context");
  }

  ENTER;
  SAVETMPS;

  PUSHMARK (SP);
  XPUSHs (self);

  va_start( args, num );

  for( ; num > 0; num-- ) {
    XPUSHs (va_arg( args, SV* ));
  }

  va_end( args );

  PUTBACK;

  count = call_method (method, flags | G_SCALAR);

  if (!(flags & G_DISCARD)) {
    SPAGAIN;

    if (count != 1) {
      croak ("method didn't return a value");
    }

    ret = POPs;
    SvREFCNT_inc (ret);
  }

  PUTBACK;
  FREETMPS;
  LEAVE;

  return ret;
}

SV *
perl_mongo_call_function (const char *func, int num, ...) {
  dSP;
  SV *ret;
  I32 count;
  va_list args;

  ENTER;
  SAVETMPS;

  PUSHMARK (SP);

  va_start( args, num );

  for( ; num > 0; num-- ) {
    XPUSHs (va_arg( args, SV* ));
  }

  va_end( args );

  PUTBACK;

  count = call_pv (func, G_SCALAR);

  SPAGAIN;

  if (count != 1) {
    croak ("method didn't return a value");
  }

  ret = POPs;
  SvREFCNT_inc (ret);

  PUTBACK;
  FREETMPS;
  LEAVE;

  return ret;
}


void
perl_mongo_attach_ptr_to_instance (SV *self, void *ptr, MGVTBL *vtbl)
{
  MAGIC *mg;

  mg = sv_magicext (SvRV (self), 0, PERL_MAGIC_ext, vtbl, (const char *)ptr, 0);
  mg->mg_flags |= MGf_DUP;
}

void *
perl_mongo_get_ptr_from_instance (SV *self, MGVTBL *vtbl)
{
  MAGIC *mg;

  if (!self || !SvOK (self) || !SvROK (self) || !sv_isobject (self)) {
    croak ("not an object");
  }

  for (mg = SvMAGIC (SvRV (self)); mg; mg = mg->mg_moremagic) {
    if (mg->mg_type == PERL_MAGIC_ext && mg->mg_virtual == vtbl)
      return mg->mg_ptr;
  }

  croak ("invalid object");
}

SV *
perl_mongo_construct_instance (const char *klass, ...)
{
  SV *ret;
  va_list ap;
  va_start (ap, klass);
  ret = perl_mongo_construct_instance_va (klass, ap);
  va_end(ap);
  return ret;
}

SV *
perl_mongo_construct_instance_va (const char *klass, va_list ap)
{
  dSP;
  SV *ret;
  I32 count;
  char *init_arg;

  ENTER;
  SAVETMPS;

  PUSHMARK (SP);
  mXPUSHp (klass, strlen (klass));
  while ((init_arg = va_arg (ap, char *))) {
    mXPUSHp (init_arg, strlen (init_arg));
    XPUSHs (va_arg (ap, SV *));
  }
  PUTBACK;

  count = call_method ("new", G_SCALAR);

  SPAGAIN;

  if (count != 1) {
    croak ("constructor didn't return an instance");
  }

  ret = POPs;
  SvREFCNT_inc (ret);

  PUTBACK;
  FREETMPS;
  LEAVE;

  return ret;
}

SV *
perl_mongo_construct_instance_with_magic (const char *klass, void *ptr, MGVTBL *vtbl, ...)
{
  SV *ret;
  va_list ap;

  va_start (ap, vtbl);
  ret = perl_mongo_construct_instance_va (klass, ap);
  va_end (ap);

  perl_mongo_attach_ptr_to_instance (ret, ptr, vtbl);

  return ret;
}

static SV *bson_to_av (buffer *buf);

void perl_mongo_make_oid(char *twelve, char *twenty4) {
  int i;
  char *id_str = twelve;
  char *movable = twenty4;

  for(i=0; i<12; i++) {
    int x = *id_str;
    if (*id_str < 0) {
      x = 256 + *id_str;
    }
    sprintf(movable, "%02x", x);
    movable += 2;
    id_str++;
  }
  twenty4[24] = '\0';
}

static SV *
oid_to_sv (buffer *buf)
{
  HV *stash, *id_hv;
  char oid_s[25];
  perl_mongo_make_oid(buf->pos, oid_s);

  id_hv = newHV();
  (void)hv_store(id_hv, "value", strlen("value"), newSVpvn(oid_s, 24), 0);

  stash = gv_stashpv("MongoDB::OID", 0);
  return sv_bless(newRV_noinc((SV *)id_hv), stash);
}

static SV *
elem_to_sv (int type, buffer *buf)
{
  SV *value = 0;

  SV *flag = get_sv("MongoDB::BSON::utf8_flag_on", 0);

  switch(type) {
  case BSON_OID: {
    value = oid_to_sv(buf);
    buf->pos += OID_SIZE;
    break;
  }
  case BSON_DOUBLE: {
    int64_t i = MONGO_64p(buf->pos);
    double d;

    memcpy(&d, &i, DOUBLE_64);

    value = newSVnv(d);
    buf->pos += DOUBLE_64;
    break;
  }
  case BSON_SYMBOL:
  case BSON_STRING: {
    int len = MONGO_32p(buf->pos);
    buf->pos += INT_32;

    // this makes a copy of the buffer
    // len includes \0
    value = newSVpvn(buf->pos, len-1);

    if (!flag || !SvIOK(flag) || SvIV(flag) != 0) {
      SvUTF8_on(value);
    }

    buf->pos += len;
    break;
  }
  case BSON_OBJECT: {
    value = perl_mongo_bson_to_sv(buf);
    break;
  }
  case BSON_ARRAY: {
    value = bson_to_av(buf);
    break;
  }
  case BSON_BINARY: {
    int len = MONGO_32p(buf->pos);
    unsigned char type;
    SV *use_binary = get_sv("MongoDB::BSON::use_binary", 0);

    buf->pos += INT_32;

    // we should do something with type
    type = *buf->pos++;

    if (type == SUBTYPE_BINARY_DEPRECATED) {
      int len2 = MONGO_32p(buf->pos);
      if (len2 == len - 4) {
        len = len2;
        buf->pos += INT_32;
      }
    }

    if (use_binary && SvTRUE(use_binary)) {
      SV *data = sv_2mortal(newSVpvn(buf->pos, len));
      SV *subtype = sv_2mortal(newSViv(type));
      value = perl_mongo_construct_instance("MongoDB::BSON::Binary", "data", data, "subtype", subtype, NULL);
    }
    else {
      value = newSVpvn(buf->pos, len);
    }

    buf->pos += len;
    break;
  }
  case BSON_BOOL: {
    dSP;
    char d = *buf->pos++;
    int count;
    SV *use_bool = get_sv("MongoDB::BSON::use_boolean", 0);

    if (!use_bool) {
      value = newSViv(d);
      break;
    }

    SAVETMPS;

    PUSHMARK(SP);
    PUTBACK;
    if (d) {
      count = call_pv("boolean::true", G_SCALAR);
    }
    else {
      count = call_pv("boolean::false", G_SCALAR);
    }
    SPAGAIN;
    if (count == 1)
      value = newSVsv(POPs);

    if (count != 1 || !SvOK(value)) {
      value = newSViv(d);
    }

    PUTBACK;
    FREETMPS;
    break;
  }
  case BSON_UNDEF:
  case BSON_NULL: {
    value = newSV(0);
    break;
  }
  case BSON_INT: {
    value = newSViv(MONGO_32p(buf->pos));
    buf->pos += INT_32;
    break;
  }
  case BSON_LONG: {
#if defined(USE_64_BIT_INT)
    value = newSViv(MONGO_64p(buf->pos));
#else
    value = newSVnv((double)MONGO_64p(buf->pos));
#endif
    buf->pos += INT_64;
    break;
  }
  case BSON_DATE: {
    int64_t ms_i = MONGO_64p(buf->pos);
    SV *datetime, *ms, **heval;
    HV *named_params;
    buf->pos += INT_64;
    ms_i /= 1000;

    datetime = sv_2mortal(newSVpv("DateTime", 0));
    ms = newSViv(ms_i);

    named_params = newHV();
    heval = hv_store(named_params, "epoch", strlen("epoch"), ms, 0);

    value = perl_mongo_call_function("DateTime::from_epoch", 2, datetime,
                                     sv_2mortal(newRV_inc(sv_2mortal((SV*)named_params))));
    break;
  }
  case BSON_REGEX: {
    SV *pattern, *regex_ref;
#if PERL_REVISION==5 && PERL_VERSION<12
    SV *regex;
#endif
    HV *stash;
    U32 flags = 0;
    REGEXP *re;
#if PERL_REVISION==5 && PERL_VERSION<=8
    PMOP pm;
    STRLEN len;
    char *pat;
#endif

    pattern = sv_2mortal(newSVpv(buf->pos, 0));
    buf->pos += strlen(buf->pos)+1;

    while(*(buf->pos) != 0) {
      switch(*(buf->pos)) {
      case 'l':
#if PERL_REVISION==5 && PERL_VERSION<=12
        flags |= PMf_LOCALE;
#else
        set_regex_charset(&flags, REGEX_LOCALE_CHARSET);
#endif
        break;
      case 'm':
        flags |= PMf_MULTILINE;
        break;
      case 'i':
        flags |= PMf_FOLD;
        break;
      case 'x':
        flags |= PMf_EXTENDED;
        break;
      case 's':
        flags |= PMf_SINGLELINE;
        break;
      }
      buf->pos++;
    }
    buf->pos++;

#if PERL_REVISION==5 && PERL_VERSION<=8
    /* 5.8 */
    pm.op_pmdynflags = flags;
    pat = SvPV(pattern, len);
    re = pregcomp(pat, pat + len, &pm);
#else
    /* 5.10 and beyond */
    re = re_compile(pattern, flags);
#endif
     // eo version-dependent code

#if PERL_REVISION==5 && PERL_VERSION>=12
    // they removed magic and made this a normal obj in 5.12
    regex_ref = newRV((SV*)re);
#else
    regex = sv_2mortal(newSVpv("",0));
    regex_ref = newRV((SV*)regex);

    sv_magic(regex, (SV*)re, PERL_MAGIC_qr, 0, 0);
#endif

    stash = gv_stashpv("Regexp", 0);
    sv_bless(regex_ref, stash);

    value = regex_ref;
    break;
  }
  case BSON_CODE:
  case BSON_CODE__D: {
    SV *code, *scope;
    int code_len;

    if (type == BSON_CODE) {
      buf->pos += INT_32;
    }

    code_len = MONGO_32p(buf->pos);
    buf->pos += INT_32;

    code = sv_2mortal(newSVpvn(buf->pos, code_len-1));
    buf->pos += code_len;

    if (type == BSON_CODE) {
      scope = perl_mongo_bson_to_sv(buf);
      value = perl_mongo_construct_instance("MongoDB::Code", "code", code, "scope", scope, NULL);
    }
    else {
      value = perl_mongo_construct_instance("MongoDB::Code", "code", code, NULL);
    }

    break;
  }
  case BSON_TIMESTAMP: {
    SV *sec_sv, *inc_sv;
    int sec, inc;

    inc = MONGO_32p(buf->pos);
    buf->pos += INT_32;
    sec = MONGO_32p(buf->pos);
    buf->pos += INT_32;

    sec_sv = sv_2mortal(newSViv(sec));
    inc_sv = sv_2mortal(newSViv(inc));

    value = perl_mongo_construct_instance("MongoDB::Timestamp", "sec", sec_sv, "inc", inc_sv, NULL);
    break;
  }
  case BSON_MINKEY: {
    HV *stash = gv_stashpv("MongoDB::MinKey", GV_ADD);
    value = sv_bless(newRV((SV*)newHV()), stash);
    break;
  }
  case BSON_MAXKEY: {
    HV *stash = gv_stashpv("MongoDB::MaxKey", GV_ADD);
    value = sv_bless(newRV((SV*)newHV()), stash);
    break;
  }
  default: {
    croak("type %d not supported\n", type);
    // give up, it'll be trouble if we keep going
  }
  }
  return value;
}

static SV *
bson_to_av (buffer *buf)
{
  AV *ret = newAV ();

  char type;

  // for size
  buf->pos += INT_32;

  while ((type = *buf->pos++) != 0) {
    SV *sv;

    // get past field name
    buf->pos += strlen(buf->pos) + 1;

    // get value
    if ((sv = elem_to_sv (type, buf))) {
      av_push (ret, sv);
    }
  }

  return newRV_noinc ((SV *)ret);
}

SV *
perl_mongo_bson_to_sv (buffer *buf)
{
  HV *ret = newHV();

  char type;

  // for size
  buf->pos += INT_32;

  while ((type = *buf->pos++) != 0) {
    char *name;
    SV *value;

    name = buf->pos;
    // get past field name
    buf->pos += strlen(buf->pos) + 1;

    // get value
    value = elem_to_sv(type, buf);
    if (!hv_store (ret, name, strlen (name), value, 0)) {
      croak ("failed storing value in hash");
    }
  }

  return newRV_noinc ((SV *)ret);
}

int perl_mongo_resize_buf(buffer *buf, int size) {
  int total = buf->end - buf->start;
  int used = buf->pos - buf->start;

  total = total < GROW_SLOWLY ? total*2 : total+INITIAL_BUF_SIZE;
  while (total-used < size) {
    total += size;
  }

  Renew(buf->start, total, char);
  buf->pos = buf->start + used;
  buf->end = buf->start + total;
  return total;
}

void perl_mongo_serialize_byte(buffer *buf, char b) {
  if(BUF_REMAINING <= 1) {
    perl_mongo_resize_buf(buf, 1);
  }
  *(buf->pos) = b;
  buf->pos += 1;
}

void perl_mongo_serialize_bytes(buffer *buf, const char *str, unsigned int str_len) {
  if(BUF_REMAINING <= str_len) {
    perl_mongo_resize_buf(buf, str_len);
  }
  memcpy(buf->pos, str, str_len);
  buf->pos += str_len;
}

void perl_mongo_serialize_string(buffer *buf, const char *str, unsigned int str_len) {
  if(BUF_REMAINING <= str_len+1) {
    perl_mongo_resize_buf(buf, str_len+1);
  }

  memcpy(buf->pos, str, str_len);
  // add \0 at the end of the string
  buf->pos[str_len] = 0;
  buf->pos += str_len + 1;
}

void perl_mongo_serialize_int(buffer *buf, int num) {
  int i = MONGO_32(num);

  if(BUF_REMAINING <= INT_32) {
    perl_mongo_resize_buf(buf, INT_32);
  }

  memcpy(buf->pos, &i, INT_32);
  buf->pos += INT_32;
}

void perl_mongo_serialize_long(buffer *buf, int64_t num) {
  int64_t i = MONGO_64(num);

  if(BUF_REMAINING <= INT_64) {
    perl_mongo_resize_buf(buf, INT_64);
  }

  memcpy(buf->pos, &i, INT_64);
  buf->pos += INT_64;
}

void perl_mongo_serialize_double(buffer *buf, double num) {
  int64_t dest, *dest_p;
  dest_p = &dest;
  memcpy(dest_p, &num, 8);
  dest = MONGO_64(dest);

  if(BUF_REMAINING <= DOUBLE_64) {
    perl_mongo_resize_buf(buf, DOUBLE_64);
  }

  memcpy(buf->pos, dest_p, DOUBLE_64);
  buf->pos += DOUBLE_64;
}

void perl_mongo_serialize_oid(buffer *buf, char *id) {
  int i;

  if(BUF_REMAINING <= OID_SIZE) {
    perl_mongo_resize_buf(buf, OID_SIZE);
  }

  for(i=0;i<OID_SIZE;i++) {
    char digit1 = id[i*2], digit2 = id[i*2+1];
    digit1 = digit1 >= 'a' && digit1 <= 'f' ? digit1 - 87 : digit1;
    digit1 = digit1 >= 'A' && digit1 <= 'F' ? digit1 - 55 : digit1;
    digit1 = digit1 >= '0' && digit1 <= '9' ? digit1 - 48 : digit1;

    digit2 = digit2 >= 'a' && digit2 <= 'f' ? digit2 - 87 : digit2;
    digit2 = digit2 >= 'A' && digit2 <= 'F' ? digit2 - 55 : digit2;
    digit2 = digit2 >= '0' && digit2 <= '9' ? digit2 - 48 : digit2;

    buf->pos[i] = digit1*16+digit2;
  }
  buf->pos += OID_SIZE;
}

void perl_mongo_serialize_bindata(buffer *buf, const int subtype, SV *sv)
{
  STRLEN len;
  const char *bytes = SvPVbyte (sv, len);

  if (subtype == SUBTYPE_BINARY_DEPRECATED) {
    // length of length+bindata
    perl_mongo_serialize_int(buf, len+4);
    perl_mongo_serialize_byte(buf, subtype);
    perl_mongo_serialize_int(buf, len);
  }
  else {
    perl_mongo_serialize_int(buf, len);
    perl_mongo_serialize_byte(buf, subtype);
  }

  // bindata
  perl_mongo_serialize_bytes(buf, bytes, len);
}

void perl_mongo_serialize_key(buffer *buf, const char *str, int is_insert) {
  SV *c = get_sv("MongoDB::BSON::char", 0);

  if(BUF_REMAINING <= strlen(str)+1) {
    perl_mongo_resize_buf(buf, strlen(str)+1);
  }

  if (str[0] == '\0') {
    croak("empty key name, did you use a $ with double quotes?");
  }

  if (is_insert && strchr(str, '.')) {
    croak("inserts cannot contain the . character");
  }

  if (c && SvPOK(c) && SvPV_nolen(c)[0] == str[0]) {
    *(buf->pos) = '$';
    memcpy(buf->pos+1, str+1, strlen(str)-1);
  }
  else {
    memcpy(buf->pos, str, strlen(str));
  }

  // add \0 at the end of the string
  buf->pos[strlen(str)] = 0;
  buf->pos += strlen(str) + 1;
}


/* the position is not increased, we are just filling
 * in the first 4 bytes with the size.
 */
void perl_mongo_serialize_size(char *start, buffer *buf) {
  int total = buf->pos - start;
  total = MONGO_32(total);

  memcpy(start, &total, INT_32);
}

void perl_mongo_make_id(char *id) {
  //SV *temp;
  char *data = id;

  // the pid is stored in $$
  SV *pid_s = get_sv("$", 0);
  // ...but if it's not, don't crash
  int pid = pid_s ? SvIV(pid_s) : rand();

  int inc;
  unsigned t;
  char *T, *M, *P, *I;

#ifdef USE_ITHREADS
  MUTEX_LOCK(&inc_mutex);
#endif
  inc = perl_mongo_inc++;
#ifdef USE_ITHREADS
  MUTEX_UNLOCK(&inc_mutex);
#endif

  t = (unsigned) time(0);

  T = (char*)&t;
  M = (char*)&perl_mongo_machine_id;
  P = (char*)&pid;
  I = (char*)&inc;

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

  data[9] = I[2];
  data[10] = I[1];
  data[11] = I[0];//  memcpy(data+9, I, 3);
#endif
}


/* add an _id */
static void
perl_mongo_prep(buffer *buf, AV *ids) {
  //  SV *id = perl_mongo_construct_instance ("MongoDB::OID", NULL);
  SV *id;
  HV *id_hv, *stash;
  char id_s[12], oid_s[25];

  stash = gv_stashpv("MongoDB::OID", 0);

  perl_mongo_make_id(id_s);
  set_type(buf, BSON_OID);
  perl_mongo_serialize_key(buf, "_id", 0);
  perl_mongo_serialize_bytes(buf, id_s, 12);

  perl_mongo_make_oid(id_s, oid_s);
  id_hv = newHV();
  (void)hv_store(id_hv, "value", strlen("value"), newSVpvn(oid_s, 24), 0);

  id = sv_bless(newRV_noinc((SV *)id_hv), stash);

  av_push(ids, id);
}

/**
 * checks if a ptr has been parsed already and, if not, adds it to the stack. If
 * we do have a circular ref, this function returns 0.
 */
static stackette* check_circular_ref(void *ptr, stackette *stack) {
  stackette *ette, *start = stack;

  while (stack) {
    if (ptr == stack->ptr) {
      return 0;
    }
    stack = stack->prev;
  }

  // push this onto the circular ref stack
  Newx(ette, 1, stackette);
  ette->ptr = ptr;
  // if stack has not been initialized, stack will be 0 so this will work out
  ette->prev = start;

  return ette;
}

static void
hv_to_bson (buffer *buf, SV *sv, AV *ids, stackette *stack, int is_insert)
{
  int start;
  int i = 0;
  HE *he;
  HV *hv;

  if (BUF_REMAINING <= 5) {
    perl_mongo_resize_buf(buf, 5);
  }

  /* keep a record of the starting position
   * as an offset, in case the memory is resized */
  start = buf->pos-buf->start;

  /* skip first 4 bytes to leave room for size */
  buf->pos += INT_32;

  if (!SvROK(sv)) {
    perl_mongo_serialize_null(buf);
    perl_mongo_serialize_size(buf->start+start, buf);
    return;
  }

  hv = (HV*)SvRV(sv);
  if (!(stack = check_circular_ref(hv, stack))) {
    Safefree(buf->start);
    croak("circular ref");
  }

  if (ids) {
    if(hv_exists(hv, "_id", strlen("_id"))) {
      SV **id = hv_fetch(hv, "_id", strlen("_id"), 0);
      append_sv(buf, "_id", *id, stack, is_insert);
      SvREFCNT_inc(*id);
      av_push(ids, *id);
    }
    else {
      perl_mongo_prep(buf, ids);
    }
  }


  (void)hv_iterinit (hv);
  while ((he = hv_iternext (hv))) {
    SV **hval;
    STRLEN len;
    const char *key = HePV (he, len);
	containsNullChar(key, len);

    /* if we've already added the oid field, continue */
    if (ids && strcmp(key, "_id") == 0) {
      continue;
    }

    /*
     * HeVAL doesn't return the correct value for tie(%foo, 'Tie::IxHash')
     * so we're using hv_fetch
     */
    if ((hval = hv_fetch(hv, key, len, 0)) == 0) {
      croak("could not find hash value for key %s", key);
    }
    append_sv (buf, key, *hval, stack, is_insert);
  }

  perl_mongo_serialize_null(buf);
  perl_mongo_serialize_size(buf->start+start, buf);

  // free the hv elem
  Safefree(stack);
}

static void
av_to_bson (buffer *buf, AV *av, stackette *stack, int is_insert) {
  I32 i;
  int start;

  if (!(stack = check_circular_ref(av, stack))) {
    Safefree(buf->start);
    croak("circular ref");
  }

  if (BUF_REMAINING <= 5) {
    perl_mongo_resize_buf(buf, 5);
  }

  start = buf->pos-buf->start;
  buf->pos += INT_32;

  for (i = 0; i <= av_len (av); i++) {
    SV **sv;
    SV *key = newSViv (i);
    if (!(sv = av_fetch (av, i, 0)))
      append_sv (buf, SvPV_nolen(key), newSV(0), stack, is_insert);
    else
      append_sv (buf, SvPV_nolen(key), *sv, stack, is_insert);

    SvREFCNT_dec (key);
  }

  perl_mongo_serialize_null(buf);
  perl_mongo_serialize_size(buf->start+start, buf);

  // free the av elem
  Safefree(stack);
}

static void
ixhash_to_bson(buffer *buf, SV *sv, AV *ids, stackette *stack, int is_insert) {
  int start, i;
  SV **keys_sv, **values_sv;
  AV *array, *keys, *values;

  if (BUF_REMAINING <= 5) {
    perl_mongo_resize_buf(buf, 5);
  }

  /* skip 4 bytes for size */
  start = buf->pos-buf->start;
  buf->pos += INT_32;

  /*
   * a Tie::IxHash is of the form:
   * [ {hash}, [keys], [order], 0 ]
   */
  array = (AV*)SvRV(sv);

  // check if we're in an infinite loop
  if (!(stack = check_circular_ref(array, stack))) {
    Safefree(buf->start);
    croak("circular ref");
  }

  /* keys in order, from position 1 */
  keys_sv = av_fetch(array, 1, 0);
  keys = (AV*)SvRV(*keys_sv);

  /* values in order, from position 2 */
  values_sv = av_fetch(array, 2, 0);
  values = (AV*)SvRV(*values_sv);

  if (ids) {
    /* check if the hash in position 0 contains an _id */
    SV **hash_sv = av_fetch(array, 0, 0);
    if (hv_exists((HV*)SvRV(*hash_sv), "_id", strlen("_id"))) {
      /*
       * if so, the value of the _id key is its index
       * in the values array.
       */
      SV **index = hv_fetch((HV*)SvRV(*hash_sv), "_id", strlen("_id"), 0);
      SV **id = av_fetch(values, SvIV(*index), 0);
      /*
       * add it to the bson and the ids array
       */
      append_sv(buf, "_id", *id, stack, is_insert);
      SvREFCNT_inc(*id);
      av_push(ids, *id);
    }
    else {
      perl_mongo_prep(buf, ids);
    }
  }

  for (i=0; i<=av_len(keys); i++) {
    SV **k, **v;
    STRLEN len;
    const char *str;

    if (!(k = av_fetch(keys, i, 0)) ||
        !(v = av_fetch(values, i, 0))) {
      croak ("failed to fetch associative array value");
    }

    str = SvPV(*k, len);

	containsNullChar(str,len);

    if (isUTF8(str, len)) {
      str = SvPVutf8(*k, len);
    }

    append_sv(buf, str, *v, stack, is_insert);
  }

  perl_mongo_serialize_null(buf);
  perl_mongo_serialize_size(buf->start+start, buf);

  // free the ixhash elem
  Safefree(stack);
}

void containsNullChar(const char* str, int len) {
  if(strlen(str)  < len)
    croak("key contains null char");
}

static int isUTF8(const char *s, int len) {
  int i;

  for (i=0; i<len; i++) {
    if (i+3 < len &&
        (s[i] & 248) == 240 &&
        (s[i+1] & 192) == 128 &&
        (s[i+2] & 192) == 128 &&
        (s[i+3] & 192) == 128) {
      i += 3;
    }
    else if (i+2 < len &&
             (s[i] & 240) == 224 &&
             (s[i+1] & 192) == 128 &&
             (s[i+2] & 192) == 128) {
      i += 2;
    }
    else if (i+1 < len &&
             (s[i] & 224) == 192 &&
             (s[i+1] & 192) == 128) {
      i += 1;
    }
    else if ((s[i] & 128) != 0) {
      return 0;
    }
  }
  return 1;
}


static void
append_sv (buffer *buf, const char *key, SV *sv, stackette *stack, int is_insert)
{
  if (!SvOK(sv)) {
    if (SvGMAGICAL(sv)) {
      mg_get(sv);
    }
    else {
      set_type(buf, BSON_NULL);
      perl_mongo_serialize_key(buf, key, is_insert);
      return;
    }
  }

  if (SvROK (sv)) {
    if (sv_isobject (sv)) {
      /* OIDs */
      if (sv_derived_from (sv, "MongoDB::OID")) {
        SV *attr = perl_mongo_call_reader (sv, "value");
        char *str = SvPV_nolen (attr);

        set_type(buf, BSON_OID);
        perl_mongo_serialize_key(buf, key, is_insert);
        perl_mongo_serialize_oid(buf, str);

        SvREFCNT_dec (attr);
      }
      /* 64-bit integers */
      else if (sv_isa(sv, "Math::BigInt")) {
        int64_t big = 0, offset = 1;
        int i = 0, length = 0, sign = 1;
        SV **av_ref, **sign_ref;
        AV *av;

        set_type(buf, BSON_LONG);
        perl_mongo_serialize_key(buf, key, is_insert);

        // get sign
        sign_ref = hv_fetch((HV*)SvRV(sv), "sign", strlen("sign"), 0);
        if (!sign_ref) {
          croak( "couldn't get BigInt sign" );
        }
        else if ( SvPOK(*sign_ref) && strcmp(SvPV_nolen( *sign_ref ), "-") == 0 ) {
          sign = -1;
        }

        // get value
        av_ref = hv_fetch((HV*)SvRV(sv), "value", strlen("value"), 0);
        if (!av_ref) {
          croak( "couldn't get BigInt value" );
        }

        av = (AV*)SvRV(*av_ref);

        if ( av_len( av ) > 3 ) {
          croak( "BigInt is too large" );
        }

        for (i = 0; i <= av_len( av ); i++) {
          int j = 0;
          SV **val;

          if ( !(val = av_fetch (av, i, 0)) || !(SvPOK(*val) || SvIOK(*val)) ) {
            sv_dump( sv );
            croak ("failed to fetch BigInt element");
          }

          if ( SvIOK(*val) ) {
            int64_t temp = SvIV(*val);

            while (temp > 0) {
              temp = temp / 10;
              length++;
            }

            temp = (int64_t)(((int64_t)SvIV(*val)) * (int64_t)offset);
            big = big + temp;
          }
          else {
            STRLEN len = sv_len(*val);

            length += len;
            big += ((int64_t)atoi(SvPV_nolen(*val))) * offset;
          }

          for (j = 0; j < length; j++) {
            offset *= 10;
          }
        }

        perl_mongo_serialize_long(buf, big*sign);
      }
      /* Tie::IxHash */
      else if (sv_isa(sv, "Tie::IxHash")) {
        set_type(buf, BSON_OBJECT);
        perl_mongo_serialize_key(buf, key, is_insert);
        ixhash_to_bson(buf, sv, NO_PREP, stack, is_insert);
      }
      /* DateTime */
      else if (sv_isa(sv, "DateTime")) {
        SV *sec, *ms, *tz, *tz_name;
        STRLEN len;
        char *str;
        set_type(buf, BSON_DATE);
        perl_mongo_serialize_key(buf, key, is_insert);

        // check for floating tz
        tz = perl_mongo_call_reader (sv, "time_zone");
        tz_name = perl_mongo_call_reader (tz, "name");
        str = SvPV(tz_name, len);
        if (len == 8 && strncmp("floating", str, 8) == 0) {
          warn("saving floating timezone as UTC");
        }
        SvREFCNT_dec (tz);
        SvREFCNT_dec (tz_name);

        sec = perl_mongo_call_reader (sv, "epoch");
        ms = perl_mongo_call_method (sv, "millisecond", 0, 0);

        perl_mongo_serialize_long(buf, (int64_t)SvIV(sec)*1000+SvIV(ms));

        SvREFCNT_dec (sec);
        SvREFCNT_dec (ms);
      }
      /* boolean */
      else if (sv_isa(sv, "boolean")) {
        set_type(buf, BSON_BOOL);
        perl_mongo_serialize_key(buf, key, is_insert);
        perl_mongo_serialize_byte(buf, SvIV(SvRV(sv)));
      }
      else if (sv_isa(sv, "MongoDB::Code")) {
        SV *code, *scope;
        char *code_str;
        STRLEN code_len;
        int start;

        set_type(buf, BSON_CODE);
        perl_mongo_serialize_key(buf, key, is_insert);

        if (BUF_REMAINING <= INT_32) {
          perl_mongo_resize_buf(buf, INT_32);
        }

        start = buf->pos-buf->start;
        buf->pos += INT_32;

        code = perl_mongo_call_reader (sv, "code");
        code_str = SvPV(code, code_len);
        perl_mongo_serialize_int(buf, code_len+1);
        perl_mongo_serialize_string(buf, code_str, code_len);

        scope = perl_mongo_call_method (sv, "scope", 0, 0);
        hv_to_bson(buf, scope, NO_PREP, EMPTY_STACK, is_insert);

        perl_mongo_serialize_size(buf->start+start, buf);

        SvREFCNT_dec(code);
        SvREFCNT_dec(scope);
      }
      else if (sv_isa(sv, "MongoDB::Timestamp")) {
        SV *sec, *inc;
        set_type(buf, BSON_TIMESTAMP);
        perl_mongo_serialize_key(buf, key, is_insert);

        inc = perl_mongo_call_reader(sv, "inc");
        perl_mongo_serialize_int(buf, SvIV(inc));
        sec = perl_mongo_call_reader(sv, "sec");
        perl_mongo_serialize_int(buf, SvIV(sec));

        SvREFCNT_dec(sec);
        SvREFCNT_dec(inc);
      }
      else if (sv_isa(sv, "MongoDB::MinKey")) {
        set_type(buf, BSON_MINKEY);
        perl_mongo_serialize_key(buf, key, is_insert);
      }
      else if (sv_isa(sv, "MongoDB::MaxKey")) {
        set_type(buf, BSON_MAXKEY);
        perl_mongo_serialize_key(buf, key, is_insert);
      }
      else if (sv_isa(sv, "MongoDB::BSON::String")) {
        SV *str_sv;
        char *str;
        STRLEN str_len;

        str_sv = SvRV(sv);

        // check type ok
        if (!SvPOK(str_sv)) {
          croak("MongoDB::BSON::String must be a blessed string reference");
        }

        str = SvPV(str_sv, str_len);

        set_type(buf, BSON_STRING);
        perl_mongo_serialize_key(buf, key, is_insert);
        perl_mongo_serialize_int(buf, str_len+1);
        perl_mongo_serialize_string(buf, str, str_len);
      }
      else if (sv_isa(sv, "MongoDB::BSON::Binary")) {
        SV *data, *subtype;

        set_type(buf, BSON_BINARY);
        perl_mongo_serialize_key(buf, key, is_insert);

        subtype = perl_mongo_call_reader(sv, "subtype");
        data = perl_mongo_call_reader(sv, "data");
        perl_mongo_serialize_bindata(buf, SvIV(subtype), data);

        SvREFCNT_dec(subtype);
        SvREFCNT_dec(data);
      }
#if PERL_REVISION==5 && PERL_VERSION>=12
      // Perl 5.12 regexes
      else if (sv_isa(sv, "Regexp")) {
        REGEXP * re = SvRX(sv);

        serialize_regex(buf, key, re, is_insert);
        serialize_regex_flags(buf, sv);
      }
#endif
      else if (SvTYPE(SvRV(sv)) == SVt_PVMG) {

        MAGIC *remg;

        /* regular expression */
        if ((remg = mg_find((SV*)SvRV(sv), PERL_MAGIC_qr)) != 0) {
          REGEXP *re = (REGEXP *) remg->mg_obj;

          serialize_regex(buf, key, re, is_insert);
          serialize_regex_flags(buf, sv);
        }
        else {
          /* binary */
          set_type(buf, BSON_BINARY);
          perl_mongo_serialize_key(buf, key, is_insert);
          perl_mongo_serialize_bindata(buf, SUBTYPE_BINARY, SvRV(sv));
        }
      }
      else {
        croak ("type (%s) unhandled", HvNAME(SvSTASH(SvRV(sv))));
      }
    } else {
      switch (SvTYPE (SvRV (sv))) {
      case SVt_PVHV:
        /* hash */
        set_type(buf, BSON_OBJECT);
        perl_mongo_serialize_key(buf, key, is_insert);
        /* don't add a _id to inner objs */
        hv_to_bson (buf, sv, NO_PREP, stack, is_insert);
        break;
      case SVt_PVAV:
        /* array */
        set_type(buf, BSON_ARRAY);
        perl_mongo_serialize_key(buf, key, is_insert);
        av_to_bson (buf, (AV *)SvRV (sv), stack, is_insert);
        break;
      case SVt_PV:
        /* binary */
        set_type(buf, BSON_BINARY);
        perl_mongo_serialize_key(buf, key, is_insert);
        perl_mongo_serialize_bindata(buf, SUBTYPE_BINARY, SvRV(sv));
        break;
      default:
        sv_dump(SvRV(sv));
        croak ("type (ref) unhandled");
      }
    }
  } else {
    int is_string = 0, aggressively_number = 0;
    SV *look_for_numbers = 0;

#if PERL_REVISION==5 && PERL_VERSION<=10
    /* Flags usage changed in Perl 5.10.1.  In Perl 5.8, there is no way to
       tell from flags whether something is a string or an int!
       Therefore, for 5.8, we check:

       if (isString(sv) and number(sv) == 0 and string(sv) != '0') {
       return string;
       }
       else {
       return number;
       }

       This will incorrectly return '0' as a number in 5.8.
    */
    if (SvPOK(sv) && ((SvNOK(sv) && SvNV(sv) == 0) ||
                      (SvIOK(sv) && SvIV(sv) == 0)) &&
        strcmp(SvPV_nolen(sv), "0") != 0) {
      is_string = 1;
    }
#endif

    look_for_numbers = get_sv("MongoDB::BSON::looks_like_number", 0);
    if (look_for_numbers && SvIOK(look_for_numbers) && SvIV(look_for_numbers)) {
      aggressively_number = looks_like_number(sv);
    }

    switch (SvTYPE (sv)) {
      /* double */
    case SVt_PV:
    case SVt_NV:
    case SVt_PVNV: {
      if ((aggressively_number & IS_NUMBER_NOT_INT) || (!is_string && SvNOK(sv))) {
        set_type(buf, BSON_DOUBLE);
        perl_mongo_serialize_key(buf, key, is_insert);
        perl_mongo_serialize_double(buf, (double)SvNV (sv));
        break;
      }
    }
      /* int */
    case SVt_IV:
    case SVt_PVIV:
    case SVt_PVLV:
    case SVt_PVMG: {
      if ((aggressively_number & IS_NUMBER_NOT_INT) || (!is_string && SvNOK(sv))) {
        set_type(buf, BSON_DOUBLE);
        perl_mongo_serialize_key(buf, key, is_insert);
        perl_mongo_serialize_double(buf, (double)SvNV (sv));
        break;
      }

      // if it's publicly an int OR (privately an int AND not publicly a string)
      if (aggressively_number || (!is_string && (SvIOK(sv) || (SvIOKp(sv) && !SvPOK(sv))))) {
#if defined(USE_64_BIT_INT)
        set_type(buf, BSON_LONG);
        perl_mongo_serialize_key(buf, key, is_insert);
        perl_mongo_serialize_long(buf, (int64_t)SvIV(sv));
#else
        set_type(buf, BSON_INT);
        perl_mongo_serialize_key(buf, key, is_insert);
        perl_mongo_serialize_int(buf, (int)SvIV(sv));
#endif
        break;
      }

      /* string */
      if (sv_len (sv) != strlen (SvPV_nolen (sv))) {
        set_type(buf, BSON_BINARY);
        perl_mongo_serialize_key(buf, key, is_insert);
        perl_mongo_serialize_bindata(buf, SUBTYPE_BINARY, sv);
      }
      else {
        STRLEN len;
        const char *str = SvPV(sv, len);

        if (!isUTF8(str, len)) {
          str = SvPVutf8(sv, len);
        }


        set_type(buf, BSON_STRING);
        perl_mongo_serialize_key(buf, key, is_insert);
        perl_mongo_serialize_int(buf, len+1);
        perl_mongo_serialize_string(buf, str, len);
      }
      break;
    }
    default:
      sv_dump(sv);
      croak ("type (sv) unhandled");
    }
  }
}

static void serialize_regex(buffer *buf, const char *key, REGEXP *re, int is_insert) {
  set_type(buf, BSON_REGEX);
  perl_mongo_serialize_key(buf, key, is_insert);
  perl_mongo_serialize_string(buf, RX_PRECOMP(re), RX_PRELEN(re));
}

static void serialize_regex_flags(buffer *buf, SV *sv) {
  char flags[] = {0,0,0,0,0,0};
  unsigned int i = 0, f = 0;
  STRLEN string_length;
  char *string = SvPV(sv, string_length);

  /* In order to work on newer versions of perl, this will also have to handle
   * (?^...:..)  I'm not sure what to do with that though. In the interest of
   * portability of data between perl versions, it'd make sense to just
   * denormalise ^ into regular flags, but that kind of defeats the purpose of ^.
   *
   * Also, this doesn't cover all the flags available on recent perls, and nor
   * does the matching deserialisation routine.
   */
  for(i = 2; i < string_length && string[i] != '-'; i++) {
    if (string[i] == 'i' ||
        string[i] == 'm' ||
        string[i] == 'x' ||
        string[i] == 'l' ||
        string[i] == 's' ||
        string[i] == 'u') {
      flags[f++] = string[i];
    }
    else if(string[i] == ':') {
      break;
    }
  }

  perl_mongo_serialize_string(buf, flags, strlen(flags));
}


void
perl_mongo_sv_to_bson (buffer *buf, SV *sv, AV *ids) {
  if (!SvROK (sv)) {
    croak ("not a reference");
  }

  switch (SvTYPE (SvRV (sv))) {
  case SVt_PVHV:
    hv_to_bson (buf, sv, ids, EMPTY_STACK, ids != 0);
    break;
  case SVt_PVAV: {
    if (sv_isa(sv, "Tie::IxHash")) {
      ixhash_to_bson(buf, sv, ids, EMPTY_STACK, ids != 0);
    }
    else {
      /*
       * this is a special case of array:
       * ("foo" => "bar", "baz" => "bat")
       * which is, as far as i can tell,
       * indistinguishable from a "normal"
       * array.
       */

      I32 i;
      AV *av = (AV *)SvRV (sv);
      int start;

      if ((av_len (av) % 2) == 0) {
        croak ("odd number of elements in structure");
      }

      // this should never come up
      if (BUF_REMAINING <= 5) {
        perl_mongo_resize_buf(buf, 5);
      }

      start = buf->pos-buf->start;
      buf->pos += INT_32;

      /*
       * the best (and not very good) way i can think of for
       * checking for ids is to go through the array once
       * looking for them... blah
       */
      if (ids) {
        int has_id = 0;
        for (i = 0; i <= av_len(av); i+= 2) {
          SV **key = av_fetch(av, i, 0);
          if (strcmp(SvPV_nolen(*key), "_id") == 0) {
            SV **val = av_fetch(av, i+1, 0);
            has_id = 1;
            append_sv(buf, "_id", *val, EMPTY_STACK, ids != 0);
            SvREFCNT_inc(*val);
            av_push(ids, *val);
            break;
          }
        }
        if (!has_id) {
          perl_mongo_prep(buf, ids);
        }
      }

      for (i = 0; i <= av_len (av); i += 2) {
        SV **key, **val;
        STRLEN len;
        const char *str;

        if ( !((key = av_fetch (av, i, 0)) && (val = av_fetch (av, i + 1, 0))) ) {
          croak ("failed to fetch array element");
        }

        str = SvPV(*key, len);

        if (!isUTF8(str, len)) {
          str = SvPVutf8(*key, len);
        }
        append_sv (buf, str, *val, EMPTY_STACK, ids != 0);
      }

      perl_mongo_serialize_null(buf);
      perl_mongo_serialize_size(buf->start+start, buf);
    }
    break;
  }
  default:
    sv_dump(sv);
    croak ("type unhandled");
  }
}
