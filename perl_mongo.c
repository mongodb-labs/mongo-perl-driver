/*
 *  Copyright 2009-2015 MongoDB, Inc.
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

#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "regcomp.h"
#include "bson.h"

//load after other Perl headers
#include "ppport.h"

/* whether to add an _id field */
#define PREP 1
#define NO_PREP 0

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

/* convenience functions taken from Text::CSV_XS by H.M. Brand */
#define _is_arrayref(f) ( f && \
     (SvROK (f) || (SvRMAGICAL (f) && (mg_get (f), 1) && SvROK (f))) && \
      SvOK (f) && SvTYPE (SvRV (f)) == SVt_PVAV )
#define _is_hashref(f) ( f && \
     (SvROK (f) || (SvRMAGICAL (f) && (mg_get (f), 1) && SvROK (f))) && \
      SvOK (f) && SvTYPE (SvRV (f)) == SVt_PVHV )
#define _is_coderef(f) ( f && \
     (SvROK (f) || (SvRMAGICAL (f) && (mg_get (f), 1) && SvROK (f))) && \
      SvOK (f) && SvTYPE (SvRV (f)) == SVt_PVCV )

/* shorthand for getting an SV* from a hash and key */
#define _hv_fetchs_sv(h,k) \
    (((svp = hv_fetchs(h, k, FALSE)) && *svp) ? *svp : 0)

#include "perl_mongo.h"
#include "string.h"

/* perl call helpers
 *
 * For convenience, these functions encapsulate the verbose stack
 * manipulation code necessary to call perl functions from C.
 *
 */

static SV * call_method_va(SV *self, const char *method, int num, ...);
static SV * call_method_with_pairs(SV *self, const char *method, ...);
static SV * new_object_from_pairs(const char *klass, ...);
static SV * _call_method_with_pairs (SV *self, const char *method, va_list args);
static SV * call_sv_va (SV *func, int num, ...);

#define call_perl_reader(s,m) call_method_va(s,m,0)

/* BSON encoding
 *
 * Public function perl_mongo_sv_to_bson is the entry point.  It calls one of
 * the container encoding functions, hv_to_bson, ixhash_to_bson or
 * avdoc_to_bson.  Those iterate their contents, encoding them with
 * sv_to_bson_elem.  sv_to_bson_elem delegates to various append_* functions
 * for particular types.
 *
 * Other functions are utility functions used during encoding.
 */

static void hv_to_bson(bson_t * bson, SV *sv, HV *opts, stackette *stack);
static void ixhash_to_bson(bson_t * bson, SV *sv, HV *opts, stackette *stack);
static void avdoc_to_bson(bson_t * bson, SV *sv, HV *opts, stackette *stack);

static void sv_to_bson_elem (bson_t * bson, const char *key, SV *sv, HV *opts, stackette *stack);

const char * maybe_append_first_key(bson_t *bson, HV *opts, stackette *stack);

static void append_binary(bson_t * bson, const char * key, bson_subtype_t subtype, SV * sv);
static void append_regex(bson_t * bson, const char *key, REGEXP *re, SV * sv);
static void append_decomposed_regex(bson_t *bson, const char *key, const char *pattern, const char *flags);

static void assert_valid_key(const char* str, int len, HV *opts);
static const char * bson_key(const char * str, HV *opts);
static void get_regex_flags(char * flags, SV *sv);
static stackette * check_circular_ref(void *ptr, stackette *stack);

/* BSON decoding
 *
 * Public function perl_mongo_bson_to_sv is the entry point.  It calls
 * bson_doc_to_hashref, which construct a container and fills it using
 * bson_elem_to_sv.  That may call bson_doc_to_hashref or
 * bson_doc_to_arrayref to decode sub-containers.
 *
 * The bson_oid_to_sv function manually constructs a MongoDB::OID object to
 * avoid the overhead of calling its constructor.  This optimization is
 * fragile and might need to be reconsidered.
 *
 */

static SV * bson_doc_to_hashref(bson_iter_t * iter, HV *opts);
static SV * bson_array_to_arrayref(bson_iter_t * iter, HV *opts);
static SV * bson_elem_to_sv(const bson_iter_t * iter, HV *opts);
static SV * bson_oid_to_sv(const bson_iter_t * iter);

/********************************************************************
 * Some C libraries (e.g. MSVCRT) do not have a "timegm" function.
 * Here is a surrogate implementation.
 ********************************************************************/

#if defined(WIN32) || defined(sun)

static int
is_leap_year(unsigned year) {
    year += 1900;
    return (year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0);
}

static time_t
timegm(struct tm *tm) {
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

/********************************************************************
 * perl call helpers
 ********************************************************************/

/* call_method_va -- calls a method with a variable number
 * of SV * arguments.  The SV* arguments are NOT mortalized.
 * Must give the number of arguments before the variable list */

static SV *
call_method_va (SV *self, const char *method, int num, ...) {
  dSP;
  SV *ret;
  I32 count;
  va_list args;

  ENTER;
  SAVETMPS;
  PUSHMARK (SP);
  XPUSHs (self);

  va_start (args, num);
  for( ; num > 0; num-- ) {
    XPUSHs (va_arg( args, SV* ));
  }
  va_end(args);

  PUTBACK;
  count = call_method (method, G_SCALAR);

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

/* call_method_va_paris -- calls a method with a variable number
 * of key/value pairs as paired char* and SV* arguments.  The SV* arguments
 * are NOT mortalized.  The final argument must be a NULL key. */

static SV *
call_method_with_pairs (SV *self, const char *method, ...) {
  SV *ret;
  va_list args;
  va_start (args, method);
  ret = _call_method_with_pairs(self, method, args);
  va_end(args);
  return ret;
}

/* new_object_from_pairs -- calls 'new' with a variable number of
 * of key/value pairs as paired char* and SV* arguments.  The SV* arguments
 * are NOT mortalized.  The final argument must be a NULL key. */

static SV *
new_object_from_pairs(const char *klass, ...) {
  SV *ret;
  va_list args;
  va_start (args, klass);
  ret = _call_method_with_pairs(sv_2mortal(newSVpv(klass,0)), "new", args);
  va_end(args);
  return ret;
}

static SV *
_call_method_with_pairs (SV *self, const char *method, va_list args) {
  dSP;
  SV *ret = NULL;
  char *key;
  I32 count;

  ENTER;
  SAVETMPS;
  PUSHMARK (SP);
  XPUSHs (self);

  while ((key = va_arg (args, char *))) {
    mXPUSHp (key, strlen (key));
    XPUSHs (va_arg (args, SV *));
  }

  PUTBACK;
  count = call_method (method, G_SCALAR);

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

static SV *
call_sv_va (SV *func, int num, ...) {
  dSP;
  SV *ret;
  I32 count;
  va_list args;

  ENTER;
  SAVETMPS;
  PUSHMARK (SP);

  va_start (args, num);
  for( ; num > 0; num-- ) {
    XPUSHs (va_arg( args, SV* ));
  }
  va_end(args);

  PUTBACK;
  count = call_sv(func, G_SCALAR);

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

/********************************************************************
 * BSON encoding
 ********************************************************************/

void
perl_mongo_sv_to_bson (bson_t * bson, SV *sv, HV *opts) {

  if (!SvROK (sv)) {
    croak ("not a reference");
  }

  switch (SvTYPE (SvRV (sv))) {
  case SVt_PVHV:
    hv_to_bson (bson, sv, opts, EMPTY_STACK);
    break;
  case SVt_PVAV: {
    if (sv_isa(sv, "Tie::IxHash")) {
      ixhash_to_bson(bson, sv, opts, EMPTY_STACK);
    }
    else {
      avdoc_to_bson(bson, sv, opts, EMPTY_STACK);
    }
    break;
  }
  default:
    sv_dump(sv);
    croak ("type unhandled");
  }
}

static void
hv_to_bson(bson_t * bson, SV *sv, HV *opts, stackette *stack) {
  HE *he;
  HV *hv;
  const char *first_key;

  hv = (HV*)SvRV(sv);
  if (!(stack = check_circular_ref(hv, stack))) {
    croak("circular ref");
  }

  first_key = maybe_append_first_key(bson, opts, stack);

  (void)hv_iterinit (hv);
  while ((he = hv_iternext (hv))) {
    SV **hval;
    STRLEN len;
    const char *key = HePV (he, len);
    uint32_t utf8 = HeUTF8(he);
    assert_valid_key(key, len, opts);

    /* if we've already added the first key, continue */
    if (first_key && strcmp(key, first_key) == 0) {
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

    sv_to_bson_elem (bson, key, *hval, opts, stack);
    if (!utf8) {
      Safefree(key);
    }
  }

  // free the hv elem
  Safefree(stack);
}


/* This is for an array reference of key/value pairs given as a document
 * instead of a hash reference or Tie::Ixhash, not for an array ref contained
 * within* a document.
 */
static void
avdoc_to_bson (bson_t * bson, SV *sv, HV *opts, stackette *stack) {
    I32 i;
    const char *first_key;
    AV *av = (AV *)SvRV (sv);

    if ((av_len (av) % 2) == 0) {
        croak ("odd number of elements in structure");
    }

    first_key = maybe_append_first_key(bson, opts, stack);

    /* XXX handle first key here
     */

    for (i = 0; i <= av_len (av); i += 2) {
        SV **key, **val;
        STRLEN len;
        const char *str;

        if ( !((key = av_fetch (av, i, 0)) && (val = av_fetch (av, i + 1, 0))) ) {
            croak ("failed to fetch array element");
        }

        str = SvPVutf8(*key, len);
        assert_valid_key(str, len, opts);

        if (first_key && strcmp(str, first_key) == 0) {
            continue;
        }

        sv_to_bson_elem (bson, str, *val, opts, EMPTY_STACK);
    }
}

static void
ixhash_to_bson(bson_t * bson, SV *sv, HV *opts, stackette *stack) {
  int i;
  SV **keys_sv, **values_sv;
  AV *array, *keys, *values;
  const char *first_key;

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

  first_key = maybe_append_first_key(bson, opts, stack);

  for (i=0; i<=av_len(keys); i++) {
    SV **k, **v;
    STRLEN len;
    const char *str;

    if (!(k = av_fetch(keys, i, 0)) ||
        !(v = av_fetch(values, i, 0))) {
      croak ("failed to fetch associative array value");
    }

    str = SvPVutf8(*k, len);
    assert_valid_key(str,len, opts);

    if (first_key && strcmp(str, first_key) == 0) {
        continue;
    }

    sv_to_bson_elem(bson, str, *v, opts, stack);
  }

  // free the ixhash elem
  Safefree(stack);
}

/* This is for an array reference contained *within* a document */
static void
av_to_bson (bson_t * bson, AV *av, HV *opts, stackette *stack) {
  I32 i;

  if (!(stack = check_circular_ref(av, stack))) {
    croak("circular ref");
  }

  for (i = 0; i <= av_len (av); i++) {
    SV **sv;
    SV *key = sv_2mortal(newSViv (i));
    if (!(sv = av_fetch (av, i, 0)))
      sv_to_bson_elem (bson, SvPV_nolen(key), newSV(0), opts, stack);
    else
      sv_to_bson_elem (bson, SvPV_nolen(key), *sv, opts, stack);
  }

  // free the av elem
  Safefree(stack);
}

/* verify and transform key, if necessary */
static const char *
bson_key(const char * str, HV *opts) {
  SV **svp;
  SV *tempsv;
  STRLEN len;

  /* first swap op_char if necessary */
  if (
      (tempsv = _hv_fetchs_sv(opts, "op_char"))
      && SvOK(tempsv)
      && SvPV_nolen(tempsv)[0] == str[0]
  ) {
    char *out = savepv(str);
    SAVEFREEPV(out);
    *out = '$';
    str = out;
  }

  /* then check for validity */
  if (
      (tempsv = _hv_fetchs_sv(opts, "invalid_chars"))
      && SvOK(tempsv)
      && (len = sv_len(tempsv))
  ) {
    const char *invalid = SvPV_nolen(tempsv);
    for (int i=0; i<len; i++) {
      if (strchr(str, invalid[i])) {
        croak("documents for storage cannot contain the '%c' character",invalid[i]);
      }
    }
  }

  return str;
}

static void
sv_to_bson_elem (bson_t * bson, const char * in_key, SV *sv, HV *opts, stackette *stack) {
  SV **svp;
  const char * key = bson_key(in_key,opts);

  if (!SvOK(sv)) {
    if (SvGMAGICAL(sv)) {
      mg_get(sv);
    }
    else {
      bson_append_null(bson, key, -1);
      return;
    }
  }

  if (SvROK (sv)) {
    if (sv_isobject (sv)) {
      /* OIDs */
      if (sv_derived_from (sv, "MongoDB::OID")) {
        SV *attr = sv_2mortal(call_perl_reader(sv, "value"));
        char *str = SvPV_nolen (attr);
        bson_oid_t oid;
        bson_oid_init_from_string(&oid, str);

        bson_append_oid(bson, key, -1, &oid);

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
        ixhash_to_bson(&child, sv, opts, stack);
        bson_append_document_end(bson, &child);
      }
      /* DateTime */
      else if (sv_isa(sv, "DateTime")) {
        SV *sec, *ms, *tz, *tz_name;
        STRLEN len;
        char *str;

        // check for floating tz
        tz = sv_2mortal(call_perl_reader (sv, "time_zone"));
        tz_name = sv_2mortal(call_perl_reader (tz, "name"));
        str = SvPV(tz_name, len);
        if (len == 8 && strncmp("floating", str, 8) == 0) {
          warn("saving floating timezone as UTC");
        }

        sec = sv_2mortal(call_perl_reader (sv, "epoch"));
        ms = sv_2mortal(call_perl_reader(sv, "millisecond"));

        bson_append_date_time(bson, key, -1, (int64_t)SvIV(sec)*1000+SvIV(ms));
      }
      /* DateTime::TIny */
      else if (sv_isa(sv, "DateTime::Tiny")) { 
        struct tm t;
        time_t epoch_secs = time(NULL);
        int64_t epoch_ms;

        t.tm_year   = SvIV( sv_2mortal(call_perl_reader( sv, "year"    )) ) - 1900;
        t.tm_mon    = SvIV( sv_2mortal(call_perl_reader( sv, "month"   )) ) -    1;
        t.tm_mday   = SvIV( sv_2mortal(call_perl_reader( sv, "day"     )) )       ;
        t.tm_hour   = SvIV( sv_2mortal(call_perl_reader( sv, "hour"    )) )       ;
        t.tm_min    = SvIV( sv_2mortal(call_perl_reader( sv, "minute"  )) )       ;
        t.tm_sec    = SvIV( sv_2mortal(call_perl_reader( sv, "second"  )) )       ;
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
        dbref = sv_2mortal(call_perl_reader(sv, "_ordered"));
        bson_append_document_begin(bson, key, -1, &child);
        ixhash_to_bson(&child, dbref, opts, stack);
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

        code = sv_2mortal(call_perl_reader (sv, "code"));
        code_str = SvPV(code, code_len);
        scope = sv_2mortal(call_perl_reader(sv, "scope"));

        if (SvOK(scope)) {
            bson_t * child = bson_new();
            hv_to_bson(child, scope, opts, EMPTY_STACK);
            bson_append_code_with_scope(bson, key, -1, code_str, child);
            bson_destroy(child);
        } else {
            bson_append_code(bson, key, -1, code_str);
        }

      }
      else if (sv_isa(sv, "MongoDB::Timestamp")) {
        SV *sec, *inc;

        inc = sv_2mortal(call_perl_reader(sv, "inc"));
        sec = sv_2mortal(call_perl_reader(sv, "sec"));

        bson_append_timestamp(bson, key, -1, SvIV(sec), SvIV(inc));
      }
      else if (sv_isa(sv, "MongoDB::MinKey")) {
        bson_append_minkey(bson, key, -1);
      }
      else if (sv_isa(sv, "MongoDB::MaxKey")) {
        bson_append_maxkey(bson, key, -1);
      }
      else if (sv_isa(sv, "MongoDB::BSON::Raw")) {
        SV *str_sv;
        char *str;
        STRLEN str_len;
        bson_t *child;

        str_sv = SvRV(sv);

        // check type ok
        if (!SvPOK(str_sv)) {
          croak("MongoDB::BSON::Raw must be a blessed string reference");
        }

        str = SvPV(str_sv, str_len);

        child = bson_new_from_data((uint8_t*) str, str_len);
        bson_append_document(bson, key, -1, child);
        bson_destroy(child);
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

        subtype = sv_2mortal(call_perl_reader(sv, "subtype"));
        data = sv_2mortal(call_perl_reader(sv, "data"));

        append_binary(bson, key, SvIV(subtype), data);
      }
#if PERL_REVISION==5 && PERL_VERSION>=12
      // Perl 5.12 regexes
      else if (sv_isa(sv, "Regexp")) {
        REGEXP * re = SvRX(sv);

        append_regex(bson, key, re, sv);
      }
#endif
      else if (SvTYPE(SvRV(sv)) == SVt_PVMG) {

        MAGIC *remg;

        /* regular expression */
        if ((remg = mg_find((SV*)SvRV(sv), PERL_MAGIC_qr)) != 0) {
          REGEXP *re = (REGEXP *) remg->mg_obj;

          append_regex(bson, key, re, sv);
        }
        else {
          /* binary */

          append_binary(bson, key, BSON_SUBTYPE_BINARY, SvRV(sv));
        }
      }
      else if (sv_isa(sv, "MongoDB::BSON::Regexp") ) { 
        /* Abstract regexp object */
        SV *pattern, *flags;
        pattern = sv_2mortal(call_perl_reader( sv, "pattern" ));
        flags   = sv_2mortal(call_perl_reader( sv, "flags" ));
        
        append_decomposed_regex( bson, key, SvPV_nolen( pattern ), SvPV_nolen( flags ) );
      }
      else {
        croak ("type (%s) unhandled", HvNAME(SvSTASH(SvRV(sv))));
      }
    } else {
      SV *deref = SvRV(sv);
      switch (SvTYPE (deref)) {
      case SVt_PVHV: {
        /* hash */
        bson_t child;
        bson_append_document_begin(bson, key, -1, &child);
        /* don't add a _id to inner objs */
        hv_to_bson (&child, sv, opts, stack);
        bson_append_document_end(bson, &child);
        break;
      }
      case SVt_PVAV: {
        /* array */
        bson_t child;
        bson_append_array_begin(bson, key, -1, &child);
        av_to_bson (&child, (AV *)SvRV (sv), opts, stack);
        bson_append_array_end(bson, &child);
        break;
      }
      default: {
          if ( SvPOK(deref) ) {
            /* binary */
            append_binary(bson, key, BSON_SUBTYPE_BINARY, deref);
          }
          else {
            sv_dump(deref);
            croak ("type (ref) unhandled");
          }
        }
      }
    }
  } else {
    SV *tempsv;
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

    if ( (tempsv = _hv_fetchs_sv(opts, "prefer_numeric")) && SvTRUE (tempsv) ) {
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
        append_binary(bson, key, SUBTYPE_BINARY, sv);
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

}

const char *
maybe_append_first_key(bson_t *bson, HV *opts, stackette *stack) {
  SV *tempsv;
  SV **svp;
  const char *first_key;

  if ( (tempsv = _hv_fetchs_sv(opts, "first_key")) && SvOK (tempsv) ) {
    STRLEN len;
    first_key = SvPVutf8(tempsv, len);
    assert_valid_key(first_key, len, opts);
    if ( (tempsv = _hv_fetchs_sv(opts, "first_value")) ) {
      sv_to_bson_elem(bson, first_key, tempsv, opts, stack);
    }
    else {
      bson_append_null(bson, first_key, -1);
    }
  }

  return first_key;
}

static void
append_decomposed_regex(bson_t *bson, const char *key, const char *pattern, const char *flags ) {
  size_t pattern_length = strlen( pattern );
  char *buf;

  Newx(buf, pattern_length + 1, char );
  Copy(pattern, buf, pattern_length, char );
  buf[ pattern_length ] = '\0';
  bson_append_regex(bson, key, -1, buf, flags);
  Safefree(buf);
}

static void
append_regex(bson_t * bson, const char *key, REGEXP *re, SV * sv) {
  char flags[]     = {0,0,0,0,0};
  char * buf;
  get_regex_flags(flags, sv);

  Newx(buf, (RX_PRELEN(re) + 1), char );
  Copy(RX_PRECOMP(re), buf, RX_PRELEN(re), char );
  buf[RX_PRELEN(re)] = '\0';

  bson_append_regex(bson, key, -1, buf, flags);

  Safefree(buf);
}

static void
append_binary(bson_t * bson, const char * key, bson_subtype_t subtype, SV * sv) {
    STRLEN len;
    uint8_t * bytes = (uint8_t *) SvPVbyte(sv, len);

    bson_append_binary(bson, key, -1, subtype, bytes, len);
}

static void
assert_valid_key(const char* str, int len, HV *opts) {
  if(strlen(str)  < len) {
    croak("key contains null char");
  }
  if (len == 0) {
    croak("empty key name, did you use a $ with double quotes?");
  }

}

static void
get_regex_flags(char * flags, SV *sv) {
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
  /* 5.10 added an API to extract flags, so we use that */
  int ret_count;
  SV *flags_sv;
  SV *pat_sv;
  char *flags_tmp;
  dSP;
  ENTER;
  SAVETMPS;
  PUSHMARK (SP);
  XPUSHs (sv);
  PUTBACK;

  ret_count = call_pv( "re::regexp_pattern", G_ARRAY );
  SPAGAIN;

  if ( ret_count != 2 ) {
    croak( "error introspecting regex" );
  }

  // regexp_pattern returns two items (in list context), the pattern and a list of flags
  flags_sv = POPs;
  pat_sv   = POPs; // too bad we throw this away

  flags_tmp = SvPVutf8_nolen(flags_sv);
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

  PUTBACK;
  FREETMPS;
  LEAVE;
#endif
}

/**
 * checks if a ptr has been parsed already and, if not, adds it to the stack. If
 * we do have a circular ref, this function returns 0.
 */
static stackette*
check_circular_ref(void *ptr, stackette *stack) {
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

/********************************************************************
 * BSON decoding
 ********************************************************************/

SV *
perl_mongo_bson_to_sv (const bson_t * bson, HV *opts) {
  bson_iter_t iter;

  if ( ! bson_iter_init(&iter, bson) ) {
      croak( "error creating BSON iterator" );
  }

  return bson_doc_to_hashref(&iter, opts);
}

static SV *
bson_doc_to_hashref(bson_iter_t * iter, HV *opts) {
  SV **svp;
  SV *cb;
  SV *ret;
  HV *hv = newHV();

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

    /* get value and store into hash */
    value = bson_elem_to_sv(iter, opts);
    if (!hv_store (hv, name, 0-strlen(name), value, 0)) {
      croak ("failed storing value in hash");
    }
  }

  ret = newRV_noinc ((SV *)hv);

  /* XXX shouldn't need to limit to size 3 */
  if ( key_num == 3 && is_dbref == 1
      && (cb = _hv_fetchs_sv(opts, "dbref_callback")) && SvOK(cb)
  ) {
    SV *dbref = call_sv_va(cb, 1, ret);
    return dbref;
  }

  return ret;
}

static SV *
bson_array_to_arrayref(bson_iter_t * iter, HV *opts) {
  AV *ret = newAV ();

  while (bson_iter_next(iter)) {
    SV *sv;

    // get value
    if ((sv = bson_elem_to_sv(iter, opts ))) {
      av_push (ret, sv);
    }
  }

  return newRV_noinc ((SV *)ret);
}

static SV *
bson_elem_to_sv (const bson_iter_t * iter, HV *opts ) {
  SV **svp;
  SV *value = 0;

  switch(bson_iter_type(iter)) {
  case BSON_TYPE_OID: {
    value = bson_oid_to_sv(iter);
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
    SvUTF8_on(value);

    break;
  }
  case BSON_TYPE_DOCUMENT: {
    bson_iter_t child;
    bson_iter_recurse(iter, &child);

    value = bson_doc_to_hashref(&child, opts);

    break;
  }
  case BSON_TYPE_ARRAY: {
    bson_iter_t child;
    bson_iter_recurse(iter, &child);

    value = bson_array_to_arrayref(&child, opts);

    break;
  }
  case BSON_TYPE_BINARY: {
    const char * buf;
    uint32_t len;
    bson_subtype_t type;
    bson_iter_binary(iter, &type, &len, (const uint8_t **)&buf);

    SV *data = sv_2mortal(newSVpvn(buf, len));
    SV *subtype = sv_2mortal(newSViv(type));
    value = new_object_from_pairs("MongoDB::BSON::Binary", "data", data, "subtype", subtype, NULL
    );

    break;
  }
  case BSON_TYPE_BOOL: {
    dSP;
    bool d = bson_iter_bool(iter);
    int count;

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
    SV *as_str = sv_2mortal(newSVpv(buf,0));
    SV *big_int = sv_2mortal(newSVpvs("Math::BigInt"));
    sprintf(buf,"%" PRIi64,bson_iter_int64(iter));
    load_module(0,big_int,NULL,NULL);
    value = call_method_va(big_int, "new", 1, as_str);
#endif
    break;
  }
  case BSON_TYPE_DATE_TIME: {
    double ms_i = bson_iter_date_time(iter);

    SV *datetime, *ms, *tempsv;
    HV *named_params;
    const char *dt_type;

    ms_i /= 1000.0;

    if ( (tempsv = _hv_fetchs_sv(opts, "dt_type")) && SvOK(tempsv) ) {
      dt_type = SvPV_nolen(tempsv);
    }

    if ( dt_type == NULL ) { 
      // raw epoch
      value = newSViv(ms_i);
    } else if ( strcmp( dt_type, "DateTime::Tiny" ) == 0 ) {
      time_t epoch;
      struct tm *dt;
      epoch = bson_iter_time_t(iter);
      dt = gmtime( &epoch );

      value = new_object_from_pairs(
        dt_type,
        "year",   newSViv( dt->tm_year + 1900 ),
        "month",  newSViv( dt->tm_mon  +    1 ),
        "day",    newSViv( dt->tm_mday ),
        "hour",   newSViv( dt->tm_hour ),
        "minute", newSViv( dt->tm_min ),
        "second", newSViv( dt->tm_sec ),
        NULL
      );
    } else if ( strcmp( dt_type, "DateTime" ) == 0 ) {
      SV *epoch = sv_2mortal(newSVnv(ms_i));
      value = call_method_with_pairs(
        sv_2mortal(newSVpv(dt_type,0)), "from_epoch", "epoch", epoch, NULL
      );
    } else {
      croak( "Invalid dt_type \"%s\"", dt_type );
    }

    break;
  }
  case BSON_TYPE_REGEX: {
    SV *pattern, *regex_ref, *tempsv;
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

    if ( (tempsv = _hv_fetchs_sv(opts, "inflate_regexps")) && SvTRUE(tempsv) ) {
      /* make a MongoDB::BSON::Regexp object instead of a native Perl regexp. */
      value = new_object_from_pairs(
        "MongoDB::BSON::Regexp", "pattern", pattern, "flags", sv_2mortal( newSVpv( options, 0 ) ), NULL
      );

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
    SV *code_sv;

    code = bson_iter_code(iter, &len);

    code_sv = sv_2mortal(newSVpvn(code, len));

    value = new_object_from_pairs("MongoDB::Code", "code", code_sv, NULL);

    break;
  }
  case BSON_TYPE_CODEWSCOPE: {
    const char * code;
    const uint8_t * scope;
    uint32_t code_len, scope_len;
    SV * code_sv;
    SV * scope_sv;
    bson_t bson;
    bson_iter_t child;

    code = bson_iter_codewscope(iter, &code_len, &scope_len, &scope);
    code_sv = sv_2mortal(newSVpvn(code, code_len));

    if ( ! ( bson_init_static(&bson, scope, scope_len) && bson_iter_init(&child, &bson) ) ) {
        croak("error iterating BSON type %d\n", bson_iter_type(iter));
    }

    scope_sv = bson_doc_to_hashref(&child, opts);
    value = new_object_from_pairs("MongoDB::Code", "code", code_sv, "scope", scope_sv, NULL);

    break;
  }
  case BSON_TYPE_TIMESTAMP: {
    SV *sec_sv, *inc_sv;
    uint32_t sec, inc;

    bson_iter_timestamp(iter, &sec, &inc);

    sec_sv = sv_2mortal(newSViv(sec));
    inc_sv = sv_2mortal(newSViv(inc));

    value = new_object_from_pairs("MongoDB::Timestamp", "sec", sec_sv, "inc", inc_sv, NULL);
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
bson_oid_to_sv (const bson_iter_t * iter) {
  HV *stash, *id_hv;
  char oid_s[25];

  const bson_oid_t * oid = bson_iter_oid(iter);
  bson_oid_to_string(oid, oid_s);

  id_hv = newHV();
  (void)hv_stores(id_hv, "value", newSVpvn(oid_s, 24));

  stash = gv_stashpv("MongoDB::OID", 0);
  return sv_bless(newRV_noinc((SV *)id_hv), stash);
}

/* vim: set ts=2 sts=2 sw=2 et tw=75: */
