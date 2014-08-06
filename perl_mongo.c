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

#ifdef WIN32
#include <memory.h>
#endif

#include "regcomp.h"

static stackette* check_circular_ref(void *ptr, stackette *stack);
static void serialize_regex_obj(bson_t *bson, const char *key, const char *pattern, const char *flags);
static void serialize_regex(bson_t *, const char*, REGEXP*, SV *);
static void serialize_regex_flags(char*, SV*);
static void serialize_binary(bson_t * bson, const char * key, bson_subtype_t subtype, SV * sv);
static void append_sv (bson_t * bson, const char *key, SV *sv, stackette *stack, int is_insert);
static void containsNullChar(const char* str, int len);
static SV *bson_to_sv (bson_iter_t * iter, char *dt_type, int inflate_dbrefs, int inflate_regexps, SV *client);

#ifdef USE_ITHREADS
static perl_mutex inc_mutex;
#endif

static int perl_mongo_inc = 0;
int perl_mongo_machine_id;

static SV *utf8_flag_on;
static SV *use_binary;
static SV *use_boolean;
static SV *special_char;
static SV *look_for_numbers;

void perl_mongo_init() {
  MUTEX_INIT(&inc_mutex);
  utf8_flag_on = get_sv("MongoDB::BSON::utf8_flag_on", 0);
  use_binary = get_sv("MongoDB::BSON::use_binary", 0);
  use_boolean = get_sv("MongoDB::BSON::use_boolean", 0);
  special_char = get_sv("MongoDB::BSON::char", 0);
  look_for_numbers = get_sv("MongoDB::BSON::looks_like_number", 0);
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

static void perl_mongo_regex_flags( char *flags_ptr, SV *re ) {
  dSP;
  ENTER;
  SAVETMPS;
  PUSHMARK (SP);
  XPUSHs (re);
  PUTBACK;

  int ret_count = call_pv( "re::regexp_pattern", G_ARRAY );
  SPAGAIN;

  if ( ret_count != 2 ) { 
    croak( "error introspecting regex" );
  }

  // regexp_pattern returns two items (in list context), the pattern and a list of flags
  SV *flags_sv = POPs;
  SV *pat_sv   = POPs;

  char *flags = SvPVutf8_nolen(flags_sv);

  strncpy( flags_ptr, flags, 7 );
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
  void *p = perl_mongo_maybe_get_ptr_from_instance(self, vtbl);

  if ( ! p ) {
    croak ("invalid object");
  }

  return p;
}

void *
perl_mongo_maybe_get_ptr_from_instance (SV *self, MGVTBL *vtbl)
{
  MAGIC *mg;

  if (!self || !SvOK (self) || !SvROK (self) || !sv_isobject (self)) {
    croak ("not an object");
  }

  for (mg = SvMAGIC (SvRV (self)); mg; mg = mg->mg_moremagic) {
    if (mg->mg_type == PERL_MAGIC_ext && mg->mg_virtual == vtbl)
      return mg->mg_ptr;
  }

  return NULL;
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

SV *
perl_mongo_construct_instance_single_arg (const char *klass, SV *arg)
{
  dSP;
  SV *ret;
  I32 count;

  ENTER;
  SAVETMPS;

  PUSHMARK (SP);
  mXPUSHp (klass, strlen (klass));
  XPUSHs(arg);
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


static SV *bson_to_av (bson_iter_t * iter, char *dt_type, int inflate_dbrefs, int inflate_regexps, SV *client );

static SV *
oid_to_sv (const bson_iter_t * iter)
{
  HV *stash, *id_hv;
  char oid_s[25];

  const bson_oid_t * oid = bson_iter_oid(iter);
  bson_oid_to_string(oid, oid_s);

  id_hv = newHV();
  (void)hv_stores(id_hv, "value", newSVpvn(oid_s, 24));

  stash = gv_stashpv("MongoDB::OID", 0);
  return sv_bless(newRV_noinc((SV *)id_hv), stash);
}

static SV *
elem_to_sv (const bson_iter_t * iter, char *dt_type, int inflate_dbrefs, int inflate_regexps, SV *client )
{
  SV *value = 0;

  switch(bson_iter_type(iter)) {
  case BSON_TYPE_OID: {
    value = oid_to_sv(iter);
    break;
  }
  case BSON_TYPE_DOUBLE: {
    value = newSVnv(bson_iter_double(iter));
    break;
  }
  case BSON_TYPE_SYMBOL:
  case BSON_TYPE_UTF8: {
    const char * str;
    uint32_t len;

    if (bson_iter_type(iter) == BSON_TYPE_SYMBOL) {
      str = bson_iter_symbol(iter, &len);
    } else {
      str = bson_iter_utf8(iter, &len);
    }

    if ( ! is_utf8_string((const U8*)str,len)) {
      croak( "Invalid UTF-8 detected while decoding BSON" );
    }

    // this makes a copy of the buffer
    // len includes \0
    value = newSVpvn(str, len);

    if (!utf8_flag_on || !SvIOK(utf8_flag_on) || SvIV(utf8_flag_on) != 0) {
      SvUTF8_on(value);
    }

    break;
  }
  case BSON_TYPE_DOCUMENT: {
    bson_iter_t child;
    bson_iter_recurse(iter, &child);

    value = bson_to_sv(&child, dt_type, inflate_dbrefs, inflate_regexps, client );

    break;
  }
  case BSON_TYPE_ARRAY: {
    bson_iter_t child;
    bson_iter_recurse(iter, &child);

    value = bson_to_av(&child, dt_type, inflate_dbrefs, inflate_regexps, client );

    break;
  }
  case BSON_TYPE_BINARY: {
    const char * buf;
    uint32_t len;
    bson_subtype_t type;
    bson_iter_binary(iter, &type, &len, (const uint8_t **)&buf);

    if (use_binary && SvTRUE(use_binary)) {
      SV *data = sv_2mortal(newSVpvn(buf, len));
      SV *subtype = sv_2mortal(newSViv(type));
      value = perl_mongo_construct_instance("MongoDB::BSON::Binary", "data", data, "subtype", subtype, NULL);
    }
    else {
      value = newSVpvn(buf, len);
    }

    break;
  }
  case BSON_TYPE_BOOL: {
    dSP;
    bool d = bson_iter_bool(iter);
    int count;

    if (!use_boolean) {
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
  case BSON_TYPE_UNDEFINED:
  case BSON_TYPE_NULL: {
    value = newSV(0);
    break;
  }
  case BSON_TYPE_INT32: {
    value = newSViv(bson_iter_int32(iter));
    break;
  }
  case BSON_TYPE_INT64: {
#if defined(MONGO_USE_64_BIT_INT)
    value = newSViv(bson_iter_int64(iter));
#else
    char buf[22];
    sprintf(buf,"%" PRIi64,bson_iter_int64(iter));
    load_module(0,newSVpvs("Math::BigInt"),NULL,NULL);
    SV *as_str = sv_2mortal(newSVpv(buf,0));
    value = perl_mongo_construct_instance_single_arg("Math::BigInt", as_str);
#endif
    break;
  }
  case BSON_TYPE_DATE_TIME: {
    double ms_i = bson_iter_date_time(iter);

    SV *datetime, *ms;
    HV *named_params;
    ms_i /= 1000.0;

    if ( dt_type == NULL ) { 
      // raw epoch
      value = newSViv(ms_i);
    } else if ( strcmp( dt_type, "DateTime::Tiny" ) == 0 ) {
      datetime = sv_2mortal(newSVpv("DateTime::Tiny", 0));
      time_t epoch = bson_iter_time_t(iter);
      struct tm *dt = gmtime( &epoch );

      value = 
        perl_mongo_call_function("DateTime::Tiny::new", 13, datetime,
                                 newSVpvs("year"),
                                 newSViv( dt->tm_year + 1900 ),
                                 newSVpvs("month"),
                                 newSViv( dt->tm_mon  +    1 ),
                                 newSVpvs("day"),
                                 newSViv( dt->tm_mday ),
                                 newSVpvs("hour"),
                                 newSViv( dt->tm_hour ),
                                 newSVpvs("minute"),
                                 newSViv( dt->tm_min ),
                                 newSVpvs("second"),
                                 newSViv( dt->tm_sec )
                                 );


    } else if ( strcmp( dt_type, "DateTime" ) == 0 ) { 
      datetime = sv_2mortal(newSVpv("DateTime", 0));
      ms = newSVnv(ms_i);

      named_params = newHV();
      hv_stores(named_params, "epoch", ms);

      value = perl_mongo_call_function("DateTime::from_epoch", 2, datetime,
                                       sv_2mortal(newRV_inc(sv_2mortal((SV*)named_params))));

    } else {
      croak( "Invalid dt_type \"%s\"", dt_type );
    }

    break;
  }
  case BSON_TYPE_REGEX: {
    SV *class_str = sv_2mortal(newSVpv("MongoDB::BSON::Regexp", 0));
    SV *pattern, *regex_ref;
    const char * regex_str;
    const char * options;
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
    regex_str = bson_iter_regex(iter, &options);

    pattern = sv_2mortal(newSVpv(regex_str, 0));

    if ( inflate_regexps ) { 
      /* make a MongoDB::BSON::Regexp object instead of a native Perl regexp. */
      value = perl_mongo_call_method( class_str, "new", 0, 4,
                                      sv_2mortal( newSVpvs("pattern") ),
                                      pattern,
                                      sv_2mortal( newSVpvs("flags") ),
                                      sv_2mortal( newSVpv( options, 0 ) ) );

      break;   /* exit case */
    }


    while(*options != 0) {
      switch(*options) {
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
      options++;
    }
    options++;

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
  case BSON_TYPE_CODE: {
    const char * code;
    uint32_t len;
    
    code = bson_iter_code(iter, &len);

    SV * code_sv = sv_2mortal(newSVpvn(code, len));

    value = perl_mongo_construct_instance("MongoDB::Code", "code", code_sv, NULL);

    break;
  }
  case BSON_TYPE_CODEWSCOPE: {
    const char * code;
    const uint8_t * scope;
    uint32_t code_len, scope_len;

    code = bson_iter_codewscope(iter, &code_len, &scope_len, &scope);

    SV * code_sv = sv_2mortal(newSVpvn(code, code_len));

    bson_t bson;
    bson_iter_t child;

    if ( ! ( bson_init_static(&bson, scope, scope_len) && bson_iter_init(&child, &bson) ) ) {
        croak("error iterating BSON type %d\n", bson_iter_type(iter));
    }

    SV * scope_sv = bson_to_sv(&child, dt_type, inflate_dbrefs, inflate_regexps, client );
    value = perl_mongo_construct_instance("MongoDB::Code", "code", code_sv, "scope", scope_sv, NULL);

    break;
  }
  case BSON_TYPE_TIMESTAMP: {
    SV *sec_sv, *inc_sv;
    uint32_t sec, inc;

    bson_iter_timestamp(iter, &sec, &inc);

    sec_sv = sv_2mortal(newSViv(sec));
    inc_sv = sv_2mortal(newSViv(inc));

    value = perl_mongo_construct_instance("MongoDB::Timestamp", "sec", sec_sv, "inc", inc_sv, NULL);
    break;
  }
  case BSON_TYPE_MINKEY: {
    HV *stash = gv_stashpv("MongoDB::MinKey", GV_ADD);
    value = sv_bless(newRV((SV*)newHV()), stash);
    break;
  }
  case BSON_TYPE_MAXKEY: {
    HV *stash = gv_stashpv("MongoDB::MaxKey", GV_ADD);
    value = sv_bless(newRV((SV*)newHV()), stash);
    break;
  }
  default: {
    croak("type %d not supported\n", bson_iter_type(iter));
    // give up, it'll be trouble if we keep going
  }
  }
  return value;
}

static SV *
bson_to_av (bson_iter_t * iter, char *dt_type, int inflate_dbrefs, int inflate_regexps, SV *client )
{
  AV *ret = newAV ();

  while (bson_iter_next(iter)) {
    SV *sv;

    // get value
    if ((sv = elem_to_sv (iter, dt_type, inflate_dbrefs, inflate_regexps, client ))) {
      av_push (ret, sv);
    }
  }

  return newRV_noinc ((SV *)ret);
}

SV *
perl_mongo_buffer_to_sv(buffer * buffer, char * dt_type, int inflate_dbrefs, int inflate_regexps, SV * client)
{
  bson_reader_t * reader;
  const bson_t * bson;
  bool reached_eof;
  SV * sv;
  
  reader = bson_reader_new_from_data((uint8_t *)buffer->pos, buffer->end - buffer->pos);
  bson = bson_reader_read(reader, &reached_eof);

  sv = perl_mongo_bson_to_sv(bson, dt_type, inflate_dbrefs, inflate_regexps, client);

  buffer->pos += bson_reader_tell(reader);

  bson_reader_destroy(reader);

  return sv;
}

SV *
perl_mongo_bson_to_sv (const bson_t * bson, char *dt_type, int inflate_dbrefs, int inflate_regexps, SV *client )
{
  utf8_flag_on = get_sv("MongoDB::BSON::utf8_flag_on", 0);
  use_binary = get_sv("MongoDB::BSON::use_binary", 0);

  bson_iter_t iter;
  if ( ! bson_iter_init(&iter, bson) ) {
      croak( "error creating BSON iterator" );
  }

  return bson_to_sv(&iter, dt_type, inflate_dbrefs, inflate_regexps, client);
}

static SV *
bson_to_sv (bson_iter_t * iter, char *dt_type, int inflate_dbrefs, int inflate_regexps, SV *client )
{
  HV *ret = newHV();

  int is_dbref = 1;
  int key_num  = 0;

  while (bson_iter_next(iter)) {
    const char *name;
    SV *value;

    name = bson_iter_key(iter);

    if ( ! is_utf8_string((const U8*)name,strlen(name))) {
      croak( "Invalid UTF-8 detected while decoding BSON" );
    }

    key_num++;
    /* check if this is a DBref. We must see the keys
       $ref, $id, and $db in that order, with no extra keys */
    if ( key_num == 1 && strcmp( name, "$ref" ) ) is_dbref = 0;
    if ( key_num == 2 && is_dbref == 1 && strcmp( name, "$id" ) ) is_dbref = 0;
    if ( key_num == 3 && is_dbref == 1 && strcmp( name, "$db" ) ) is_dbref = 0;

    // get past field name

    // get value
    value = elem_to_sv(iter, dt_type, inflate_dbrefs, inflate_regexps, client );
    if (!utf8_flag_on || !SvIOK(utf8_flag_on) || SvIV(utf8_flag_on) != 0) {
    	if (!hv_store (ret, name, 0-strlen (name), value, 0)) {
     	 croak ("failed storing value in hash");
    	}
    } else {
    	if (!hv_store (ret, name, strlen (name), value, 0)) {
     	 croak ("failed storing value in hash");
    	}
    }
  }

  if ( key_num == 3 && is_dbref == 1 && inflate_dbrefs == 1 ) { 
    SV *dbr_class = sv_2mortal(newSVpv("MongoDB::DBRef", 0));
    SV *dbref = 
      perl_mongo_call_method( dbr_class, "new", 0, 8,
                              newSVpvs("ref"),
                              *hv_fetch( ret, "$ref", 4, FALSE ),
                              newSVpvs("id"),
                              *hv_fetch( ret, "$id", 3, FALSE ),
                              newSVpvs("db"),
                              *hv_fetch( ret, "$db", 3, FALSE ),
                              newSVpvs("client"),
                              client
                                 );

    return dbref;
  }

  return newRV_noinc ((SV *)ret);
}

void perl_mongo_resize_buf(buffer *buf, int size) {
  int total = buf->end - buf->start;
  int used = buf->pos - buf->start;

  total = total < GROW_SLOWLY ? total*2 : total+INITIAL_BUF_SIZE;
  while (total-used < size) {
    total += size;
  }

  Renew(buf->start, total, char);
  buf->pos = buf->start + used;
  buf->end = buf->start + total;
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

void perl_mongo_serialize_size(char *start, buffer *buf) {
  int total = buf->pos - start;
  total = MONGO_32(total);

  memcpy(start, &total, INT_32);
}

/* add an _id */
static void
perl_mongo_prep(bson_t * bson, AV *ids) {
  //  SV *id = perl_mongo_construct_instance ("MongoDB::OID", NULL);
  SV *id;
  HV *id_hv, *stash;
  bson_oid_t oid;
  char oid_s[25];

  stash = gv_stashpv("MongoDB::OID", 0);

  bson_oid_init(&oid, NULL);

  bson_append_oid(bson, "_id", -1, &oid);

  bson_oid_to_string(&oid, oid_s);
  id_hv = newHV();
  (void)hv_stores(id_hv, "value", newSVpvn(oid_s, 24));

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
hv_to_bson (bson_t * bson, SV *sv, AV *ids, stackette *stack, int is_insert)
{
  HE *he;
  HV *hv;

  hv = (HV*)SvRV(sv);
  if (!(stack = check_circular_ref(hv, stack))) {
    croak("circular ref");
  }

  if (ids) {
    if(hv_exists(hv, "_id", strlen("_id"))) {
      SV **id = hv_fetchs(hv, "_id", 0);
      append_sv(bson, "_id", *id, stack, is_insert);
      SvREFCNT_inc(*id);
      av_push(ids, *id);
    }
    else {
      perl_mongo_prep(bson, ids);
    }
  }


  (void)hv_iterinit (hv);
  while ((he = hv_iternext (hv))) {
    SV **hval;
    STRLEN len;
    const char *key = HePV (he, len);
    uint32_t utf8 = HeUTF8(he);
    containsNullChar(key, len);
    /* if we've already added the oid field, continue */
    if (ids && strcmp(key, "_id") == 0) {
      continue;
    }

    /*
     * HeVAL doesn't return the correct value for tie(%foo, 'Tie::IxHash')
     * so we're using hv_fetch
     */
    if ((hval = hv_fetch(hv, key, utf8 ? -len : len, 0)) == 0) {
      croak("could not find hash value for key %s, len:%lu", key, len);
    }
    if (!utf8) {
      key = (const char *) bytes_to_utf8((U8 *)key, &len);
    }

    if ( ! is_utf8_string((const U8*)key,len)) {
        croak( "Invalid UTF-8 detected while encoding BSON" );
    }

    append_sv (bson, key, *hval, stack, is_insert);
    if (!utf8) {
      Safefree(key);
    }
  }

  // free the hv elem
  Safefree(stack);
}

static void
av_to_bson (bson_t * bson, AV *av, stackette *stack, int is_insert) {
  I32 i;

  if (!(stack = check_circular_ref(av, stack))) {
    croak("circular ref");
  }

  for (i = 0; i <= av_len (av); i++) {
    SV **sv;
    SV *key = newSViv (i);
    if (!(sv = av_fetch (av, i, 0)))
      append_sv (bson, SvPV_nolen(key), newSV(0), stack, is_insert);
    else
      append_sv (bson, SvPV_nolen(key), *sv, stack, is_insert);

    SvREFCNT_dec (key);
  }

  // free the av elem
  Safefree(stack);
}

static void
ixhash_to_bson(bson_t * bson, SV *sv, AV *ids, stackette *stack, int is_insert) {
  int i;
  SV **keys_sv, **values_sv;
  AV *array, *keys, *values;

  /*
   * a Tie::IxHash is of the form:
   * [ {hash}, [keys], [order], 0 ]
   */
  array = (AV*)SvRV(sv);

  // check if we're in an infinite loop
  if (!(stack = check_circular_ref(array, stack))) {
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
      SV **index = hv_fetchs((HV*)SvRV(*hash_sv), "_id", 0);
      SV **id = av_fetch(values, SvIV(*index), 0);
      /*
       * add it to the bson and the ids array
       */
      append_sv(bson, "_id", *id, stack, is_insert);
      SvREFCNT_inc(*id);
      av_push(ids, *id);
    }
    else {
      perl_mongo_prep(bson, ids);
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

    str = SvPVutf8(*k, len);
    containsNullChar(str,len);
    append_sv(bson, str, *v, stack, is_insert);
  }

  // free the ixhash elem
  Safefree(stack);
}

static void containsNullChar(const char* str, int len) {
  if(strlen(str)  < len)
    croak("key contains null char");
}

 #if defined(WIN32) || defined(sun)

/* 
 * Some C libraries (e.g. MSVCRT) do not have a "timegm" function.
 * Here is a surrogate implementation.
 *
 */

static int is_leap_year(unsigned year)
{
    year += 1900;
    return (year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0);
}

time_t timegm (struct tm *tm)
{
  static const unsigned month_start[2][12] = {
	{ 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 },
	{ 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335 },
	};
  time_t ret = 0;
  int i;

  for (i = 70; i < tm->tm_year; ++i)
    ret += is_leap_year(i) ? 366 : 365;

  ret += month_start[is_leap_year(tm->tm_year)][tm->tm_mon];
  ret += tm->tm_mday - 1;
  ret *= 24;
  ret += tm->tm_hour;
  ret *= 60;
  ret += tm->tm_min;
  ret *= 60;
  ret += tm->tm_sec;
  return ret;
}

#endif /* WIN32 */

/** returns true if we need to free at the end */
const char * clean_key(const char * str, int is_insert) {
  if (str[0] == '\0') {
    croak("empty key name, did you use a $ with double quotes?");
  }

  if (is_insert && strchr(str, '.')) {
    croak("documents for storage cannot contain the . character");
  }

  if (special_char && SvPOK(special_char) && SvPV_nolen(special_char)[0] == str[0]) {
    char * out = strdup(str);

    *out = '$';

    return out;
  } else {
    return str;
  }
}

static void
append_sv (bson_t * bson, const char * in_key, SV *sv, stackette *stack, int is_insert)
{
  const char * key = clean_key(in_key, is_insert);

  if (!SvOK(sv)) {
    if (SvGMAGICAL(sv)) {
      mg_get(sv);
    }
    else {
      bson_append_null(bson, key, -1);
      if (in_key != key) free((char *)key);
      return;
    }
  }

  if (SvROK (sv)) {
    if (sv_isobject (sv)) {
      /* OIDs */
      if (sv_derived_from (sv, "MongoDB::OID")) {
        SV *attr = perl_mongo_call_reader (sv, "value");
        char *str = SvPV_nolen (attr);
        bson_oid_t oid;
        bson_oid_init_from_string(&oid, str);

        bson_append_oid(bson, key, -1, &oid);

        SvREFCNT_dec (attr);
      }
      /* 64-bit integers */
      else if (sv_isa(sv, "Math::BigInt")) {
        int64_t big = 0, offset = 1;
        int i = 0, length = 0, sign = 1;
        SV **av_ref, **sign_ref;
        AV *av;

        // get sign
        sign_ref = hv_fetchs((HV*)SvRV(sv), "sign", 0);
        if (!sign_ref) {
          croak( "couldn't get BigInt sign" );
        }
        else if ( SvPOK(*sign_ref) && strcmp(SvPV_nolen( *sign_ref ), "-") == 0 ) {
          sign = -1;
        }

        // get value
        av_ref = hv_fetchs((HV*)SvRV(sv), "value", 0);
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

        bson_append_int64(bson, key, -1, big*sign);
      }
      /* Tie::IxHash */
      else if (sv_isa(sv, "Tie::IxHash")) {
        bson_t child;

        bson_append_document_begin(bson, key, -1, &child);
        ixhash_to_bson(&child, sv, NO_PREP, stack, is_insert);
        bson_append_document_end(bson, &child);
      }
      /* DateTime */
      else if (sv_isa(sv, "DateTime")) {
        SV *sec, *ms, *tz, *tz_name;
        STRLEN len;
        char *str;

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

        bson_append_date_time(bson, key, -1, (int64_t)SvIV(sec)*1000+SvIV(ms));

        SvREFCNT_dec (sec);
        SvREFCNT_dec (ms);
      }
      /* DateTime::TIny */
      else if (sv_isa(sv, "DateTime::Tiny")) { 
        struct tm t;
        time_t epoch_secs = time(NULL);
        int64_t epoch_ms;

        t.tm_year   = SvIV( perl_mongo_call_reader( sv, "year"    ) ) - 1900;
        t.tm_mon    = SvIV( perl_mongo_call_reader( sv, "month"   ) ) -    1;
        t.tm_mday   = SvIV( perl_mongo_call_reader( sv, "day"     ) )       ;
        t.tm_hour   = SvIV( perl_mongo_call_reader( sv, "hour"    ) )       ;
        t.tm_min    = SvIV( perl_mongo_call_reader( sv, "minute"  ) )       ;
        t.tm_sec    = SvIV( perl_mongo_call_reader( sv, "second"  ) )       ;
        t.tm_isdst  = -1;     // no dst/tz info in DateTime::Tiny

        epoch_secs = timegm( &t );

        // no miliseconds in DateTime::Tiny, so just multiply by 1000
        epoch_ms = (int64_t)epoch_secs*1000;
        bson_append_date_time(bson, key, -1, epoch_ms);
      }
      /* DBRef */
      else if (sv_isa(sv, "MongoDB::DBRef")) { 
        SV *dbref;
        bson_t child;
        dbref = perl_mongo_call_reader(sv, "_ordered");

        bson_append_document_begin(bson, key, -1, &child);
        ixhash_to_bson(&child, dbref, NO_PREP, stack, is_insert);
        bson_append_document_end(bson, &child);
      }

      /* boolean */
      else if (sv_isa(sv, "boolean")) {
        bson_append_bool(bson, key, -1, SvIV(SvRV(sv)));
      }
      else if (sv_isa(sv, "MongoDB::Code")) {
        SV *code, *scope;
        char *code_str;
        STRLEN code_len;

        code = perl_mongo_call_reader (sv, "code");
        code_str = SvPV(code, code_len);
        scope = perl_mongo_call_method (sv, "scope", 0, 0);

        if (SvOK(scope)) {
            bson_t * child = bson_new();
            hv_to_bson(child, scope, NO_PREP, EMPTY_STACK, is_insert);
            bson_append_code_with_scope(bson, key, -1, code_str, child);
            bson_destroy(child);
        } else {
            bson_append_code(bson, key, -1, code_str);
        }

        SvREFCNT_dec(code);
        SvREFCNT_dec(scope);
      }
      else if (sv_isa(sv, "MongoDB::Timestamp")) {
        SV *sec, *inc;

        inc = perl_mongo_call_reader(sv, "inc");
        sec = perl_mongo_call_reader(sv, "sec");

        bson_append_timestamp(bson, key, -1, SvIV(sec), SvIV(inc));

        SvREFCNT_dec(sec);
        SvREFCNT_dec(inc);
      }
      else if (sv_isa(sv, "MongoDB::MinKey")) {
        bson_append_minkey(bson, key, -1);
      }
      else if (sv_isa(sv, "MongoDB::MaxKey")) {
        bson_append_maxkey(bson, key, -1);
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

        str = SvPVutf8(str_sv, str_len);

        if ( ! is_utf8_string((const U8*)str,str_len)) {
          croak( "Invalid UTF-8 detected while encoding BSON" );
        }

        bson_append_utf8(bson, key, -1, str, str_len);
      }
      else if (sv_isa(sv, "MongoDB::BSON::Binary")) {
        SV *data, *subtype;

        subtype = perl_mongo_call_reader(sv, "subtype");
        data = perl_mongo_call_reader(sv, "data");

        serialize_binary(bson, key, SvIV(subtype), data);

        SvREFCNT_dec(subtype);
        SvREFCNT_dec(data);
      }
#if PERL_REVISION==5 && PERL_VERSION>=12
      // Perl 5.12 regexes
      else if (sv_isa(sv, "Regexp")) {
        REGEXP * re = SvRX(sv);

        serialize_regex(bson, key, re, sv);
      }
#endif
      else if (SvTYPE(SvRV(sv)) == SVt_PVMG) {

        MAGIC *remg;

        /* regular expression */
        if ((remg = mg_find((SV*)SvRV(sv), PERL_MAGIC_qr)) != 0) {
          REGEXP *re = (REGEXP *) remg->mg_obj;

          serialize_regex(bson, key, re, sv);
        }
        else {
          /* binary */

          serialize_binary(bson, key, BSON_SUBTYPE_BINARY, SvRV(sv));
        }
      }
      else if (sv_isa(sv, "MongoDB::BSON::Regexp") ) { 
        /* Abstract regexp object */
        SV *pattern, *flags;
        pattern = perl_mongo_call_reader( sv, "pattern" );
        flags   = perl_mongo_call_reader( sv, "flags" );
        
        serialize_regex_obj( bson, key, SvPV_nolen( pattern ), SvPV_nolen( flags ) );
      }
      else {
        croak ("type (%s) unhandled", HvNAME(SvSTASH(SvRV(sv))));
      }
    } else {
      switch (SvTYPE (SvRV (sv))) {
      case SVt_PVHV: {
        /* hash */
        bson_t child;
        bson_append_document_begin(bson, key, -1, &child);
        /* don't add a _id to inner objs */
        hv_to_bson (&child, sv, NO_PREP, stack, is_insert);
        bson_append_document_end(bson, &child);
        break;
      }
      case SVt_PVAV: {
        /* array */
        bson_t child;
        bson_append_array_begin(bson, key, -1, &child);
        av_to_bson (&child, (AV *)SvRV (sv), stack, is_insert);
        bson_append_array_end(bson, &child);
        break;
      }
      case SVt_PV:
        /* binary */

        serialize_binary(bson, key, BSON_SUBTYPE_BINARY, SvRV(sv));
        break;
      default:
        sv_dump(SvRV(sv));
        croak ("type (ref) unhandled");
      }
    }
  } else {
    int is_string = 0, aggressively_number = 0;

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

    if (look_for_numbers && SvIOK(look_for_numbers) && SvIV(look_for_numbers)) {
      aggressively_number = looks_like_number(sv);
    }

    switch (SvTYPE (sv)) {
      /* double */
    case SVt_PV:
    case SVt_NV:
    case SVt_PVNV: {
      if ((aggressively_number & IS_NUMBER_NOT_INT) || (!is_string && SvNOK(sv))) {
        bson_append_double(bson, key, -1, (double)SvNV(sv));
        break;
      }
    }
      /* int */
    case SVt_IV:
    case SVt_PVIV:
    case SVt_PVLV:
    case SVt_PVMG: {
      if ((aggressively_number & IS_NUMBER_NOT_INT) || (!is_string && SvNOK(sv))) {
        bson_append_double(bson, key, -1, (double)SvNV(sv));
        break;
      }

      // if it's publicly an int OR (privately an int AND not publicly a string)
      if (aggressively_number || (!is_string && (SvIOK(sv) || (SvIOKp(sv) && !SvPOK(sv))))) {
#if defined(MONGO_USE_64_BIT_INT)
        bson_append_int64(bson, key, -1, (int64_t)SvIV(sv));
#else
        bson_append_int32(bson, key, -1, (int)SvIV(sv));
#endif
        break;
      }

      /* string */
      if (sv_len (sv) != strlen (SvPV_nolen (sv))) {
        serialize_binary(bson, key, SUBTYPE_BINARY, sv);
      }
      else {
        STRLEN len;
        const char *str = SvPVutf8(sv, len);

        if ( ! is_utf8_string((const U8*)str,len)) {
          croak( "Invalid UTF-8 detected while encoding BSON" );
        }

        bson_append_utf8(bson, key, -1, str, len);
      }
      break;
    }
    default:
      sv_dump(sv);
      croak ("type (sv) unhandled");
    }
  }

  if (in_key != key) free((char *)key);
}

static void serialize_regex_obj(bson_t *bson, const char *key, 
                                const char *pattern, const char *flags ) { 
  size_t pattern_length = strlen( pattern );
  size_t flags_length   = strlen( flags );

  char *buf = malloc( pattern_length + 1 ); 
  memcpy( buf, pattern, pattern_length );
  buf[ pattern_length ] = '\0';
  bson_append_regex(bson, key, -1, buf, flags);
  free(buf);
}

static void serialize_regex(bson_t * bson, const char *key, REGEXP *re, SV * sv) {
  char flags[]     = {0,0,0,0,0};
  serialize_regex_flags(flags, sv);

  char * buf = malloc(RX_PRELEN(re) + 1);
  memcpy(buf, RX_PRECOMP(re), RX_PRELEN(re));
  buf[RX_PRELEN(re)] = '\0';

  bson_append_regex(bson, key, -1, buf, flags);

  free(buf);
}

static void serialize_regex_flags(char * flags, SV *sv) {
  char flags_tmp[] = {0,0,0,0,0,0,0,0};
  unsigned int i = 0, f = 0;

#if PERL_REVISION == 5 && PERL_VERSION < 10
  // pre-5.10 doesn't have the re API
  STRLEN string_length;
  char *re_string = SvPV( sv, string_length );
  
  /* pre-5.14 regexes are stringified in the format: (?ix-sm:foo) where
     everything between ? and - are the current flags. The format changed
     around 5.14, but for everything after 5.10 we use the re API anyway. */
  for( i = 2; i < string_length && re_string[i] != '-'; i++ ) { 
    if ( re_string[i] == 'i'  ||
         re_string[i] == 'm'  ||
         re_string[i] == 'x'  ||
         re_string[i] == 's' ) { 
      flags[f++] = re_string[i];
    } else if ( re_string[i] == ':' ) {
      break;
    }
  }


#else
  perl_mongo_regex_flags( flags_tmp, sv );
#endif

  for ( i = 0; i < sizeof( flags_tmp ); i++ ) { 
    if ( flags_tmp[i] == 0 ) break;

    // MongoDB supports only flags /imxs, so warn if we get anything else and discard them.
    if ( flags_tmp[i] == 'i' ||
         flags_tmp[i] == 'm' ||
         flags_tmp[i] == 'x' ||
         flags_tmp[i] == 's' ) { 
      flags[f++] = flags_tmp[i];
    } else { 
      warn( "stripped unsupported regex flag /%c from MongoDB regex\n", flags_tmp[i] );
    }
  }
}

static void serialize_binary(bson_t * bson, const char * key, bson_subtype_t subtype, SV * sv)
{
    STRLEN len;
    uint8_t * bytes = (uint8_t *) SvPVbyte(sv, len);

    bson_append_binary(bson, key, -1, subtype, bytes, len);
}

void * mongo_renew(void * ptr, size_t size)
{
  Renew(ptr, size, char);

  return ptr;
}

void perl_mongo_sv_to_buffer(buffer * buf, SV *sv, AV *ids)
{
  bson_t * bson;
  bson_writer_t * writer;
  size_t buf_len;
  size_t offset;

  buf_len = buf->end - buf->start;
  offset = buf->pos - buf->start;

  writer = bson_writer_new((uint8_t **)&buf->start, &buf_len, offset, &mongo_renew);

  bson_writer_begin(writer, &bson);
  perl_mongo_sv_to_bson(bson, sv, ids!=0, ids);
  bson_writer_end(writer);

  buf->end = buf->start + buf_len;
  buf->pos = buf->start + bson_writer_get_length(writer);

  bson_writer_destroy(writer);
}

void
perl_mongo_sv_to_bson (bson_t * bson, SV *sv, int is_insert, AV *ids) {

  if (!SvROK (sv)) {
    croak ("not a reference");
  }

  special_char = get_sv("MongoDB::BSON::char", 0);
  look_for_numbers = get_sv("MongoDB::BSON::looks_like_number", 0);

  switch (SvTYPE (SvRV (sv))) {
  case SVt_PVHV:
    hv_to_bson (bson, sv, ids, EMPTY_STACK, is_insert);
    break;
  case SVt_PVAV: {
    if (sv_isa(sv, "Tie::IxHash")) {
      ixhash_to_bson(bson, sv, ids, EMPTY_STACK, is_insert);
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

      if ((av_len (av) % 2) == 0) {
        croak ("odd number of elements in structure");
      }

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
            append_sv(bson, "_id", *val, EMPTY_STACK, is_insert);
            SvREFCNT_inc(*val);
            av_push(ids, *val);
            break;
          }
        }
        if (!has_id) {
          perl_mongo_prep(bson, ids);
        }
      }

      for (i = 0; i <= av_len (av); i += 2) {
        SV **key, **val;
        STRLEN len;
        const char *str;

        if ( !((key = av_fetch (av, i, 0)) && (val = av_fetch (av, i + 1, 0))) ) {
          croak ("failed to fetch array element");
        }

        str = SvPVutf8(*key, len);

        append_sv (bson, str, *val, EMPTY_STACK, is_insert);
      }
    }
    break;
  }
  default:
    sv_dump(sv);
    croak ("type unhandled");
  }
}
