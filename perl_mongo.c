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

void
perl_mongo_call_xs (pTHX_ void (*subaddr) (pTHX_ CV *), CV *cv, SV **mark)
{
    dSP;
    PUSHMARK (mark);
    (*subaddr) (aTHX_ cv);
    PUTBACK;
}

SV *
perl_mongo_call_reader (SV *self, const char *reader)
{
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
perl_mongo_call_method (SV *self, const char *method, int num, ...)
{
    dSP;
    SV *ret;
    I32 count;
    va_list args;
    int save_num = num;

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

SV *
perl_mongo_call_function (const char *func, int num, ...)
{
    dSP;
    SV *ret;
    I32 count;
    va_list args;
    int save_num = num;

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
perl_mongo_attach_ptr_to_instance (SV *self, void *ptr)
{
    sv_magic (SvRV (self), 0, PERL_MAGIC_ext, (const char *)ptr, 0);
}

void *
perl_mongo_get_ptr_from_instance (SV *self)
{
    MAGIC *mg;

    if (!self || !SvOK (self) || !SvROK (self)
     || !(mg = mg_find (SvRV (self), PERL_MAGIC_ext))) {
        croak ("invalid object");
    }

    return mg->mg_ptr;
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
perl_mongo_construct_instance_with_magic (const char *klass, void *ptr, ...)
{
    SV *ret;
    va_list ap;

    va_start (ap, ptr);
    ret = perl_mongo_construct_instance_va (klass, ap);
    va_end (ap);

    perl_mongo_attach_ptr_to_instance (ret, ptr);

    return ret;
}

static SV *bson_to_av (buffer *buf);

void perl_mongo_oid_create(char *twelve, char *twenty4) {
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
    char id[25];
    perl_mongo_oid_create(buf->pos, id);
    return perl_mongo_construct_instance (OID_CLASS, "value", sv_2mortal(newSVpvn (id, 24)), NULL);
}

static SV *
elem_to_sv (int type, buffer *buf)
{
  SV *value;

  switch(type) {
  case BSON_OID: {
    value = oid_to_sv(buf);
    buf->pos += OID_SIZE;
    break;
  }
  case BSON_DOUBLE: {
    value = newSVnv(*(double*)buf->pos);
    buf->pos += DOUBLE_64;
    break;
  }
  case BSON_STRING: {
    int len = *((int*)buf->pos);
    char *str;
    buf->pos += INT_32;

    // this makes a copy of the buffer
    // len includes \0
    value = newSVpvn(buf->pos, len-1);
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
    int len = *(int*)buf->pos;
    char type, *bytes;

    buf->pos += INT_32;

    // we should do something with type
    type = *buf->pos++;

    if (type == 2) {
      int len2 = *(int*)buf->pos;
      if (len2 == len - 4) {
        len = len2;
        buf->pos += INT_32;
      }
    }

    value = newSVpvn(buf->pos, len);
    buf->pos += len;

    break;
  }
  case BSON_BOOL: {
    char d = *buf->pos++;
    value = newSViv(d);
    break;
  }
  case BSON_UNDEF:
  case BSON_NULL: {
    value = &PL_sv_undef;
    break;
  }
  case BSON_INT: {
    value = newSViv(*((int*)buf->pos));
    buf->pos += INT_32;
    break;
  }
  case BSON_LONG: {
    value = newSViv(*((int64_t*)buf->pos));
    buf->pos += INT_64;
    break;
  }
  case BSON_DATE: {
    int64_t ms_i = *(int64_t*)buf->pos;
    SV *datetime, *ms;
    HV *named_params;
    buf->pos += INT_64;
    ms_i /= 1000;

    datetime = sv_2mortal(newSVpv("DateTime", 0));
    ms = newSViv(ms_i);

    named_params = newHV();
    hv_store(named_params, "epoch", strlen("epoch"), ms, 0);

    value = perl_mongo_call_function("DateTime::from_epoch", 2, datetime, 
                                     sv_2mortal(newRV_inc(sv_2mortal((SV*)named_params))));
    break;
  }
  case BSON_REGEX: {
    SV *pattern, *regex, *regex_ref;
    HV *stash;
    U32 flags = 0;
    PMOP pm;
    STRLEN len;
    char *pat;
    REGEXP *re;

    pattern = sv_2mortal(newSVpv(buf->pos, 0));
    buf->pos += strlen(buf->pos)+1;

    while(*(buf->pos) != 0) {
      switch(*(buf->pos)) {
      case 'l':
        flags |= PMf_LOCALE;
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

    /* 5.10 */
#if PERL_REVISION==5 && PERL_VERSION==10
    re = re_compile(pattern, flags);
#else
    /* 5.8 */
    pm.op_pmdynflags = flags;
    pat = SvPV(pattern, len);
    re = pregcomp(pat, pat + len, &pm);
#endif
     // eo version-dependent code

    regex = sv_2mortal(newSVpv("",0));
    regex_ref = newRV((SV*)regex);
    sv_magic(regex, (SV*)re, PERL_MAGIC_qr, 0, 0);

    stash = gv_stashpv("Regexp", 0);
    sv_bless(regex_ref, stash);

    value = regex_ref;
    break;
  }
  case BSON_CODE:
  case BSON_CODE__D: {
    // TODO
    break;
  }
  case BSON_TIMESTAMP: {
    value = newSViv((long)*(int*)buf->pos);
    buf->pos += INT_64;
    break;
  }
  case BSON_MINKEY: {
    HV *stash = gv_stashpv("MongoDB::MinKey", 0);
    value = sv_bless(newRV((SV*)newHV()), stash);
    break;
  }
  case BSON_MAXKEY: {
    HV *stash = gv_stashpv("MongoDB::MaxKey", 0);
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

void perl_mongo_serialize_bytes(buffer *buf, const char *str, int str_len) {
  if(BUF_REMAINING <= str_len) {
    perl_mongo_resize_buf(buf, str_len);
  }
  memcpy(buf->pos, str, str_len);
  buf->pos += str_len;
}

void perl_mongo_serialize_string(buffer *buf, const char *str, int str_len) {
  if(BUF_REMAINING <= str_len+1) {
    perl_mongo_resize_buf(buf, str_len+1);
  }

  memcpy(buf->pos, str, str_len);
  // add \0 at the end of the string
  buf->pos[str_len] = 0;
  buf->pos += str_len + 1;
}

void perl_mongo_serialize_int(buffer *buf, int num) {
  int i;
  int *ptr = &num;

  if(BUF_REMAINING <= INT_32) {
    perl_mongo_resize_buf(buf, INT_32);
  }

  SERIALIZE(buf->pos, ptr, INT_32);
  buf->pos += INT_32;
}

void perl_mongo_serialize_long(buffer *buf, int64_t num) {
  int i;
  int64_t *ptr = &num;

  if(BUF_REMAINING <= INT_64) {
    perl_mongo_resize_buf(buf, INT_64);
  }

  SERIALIZE(buf->pos, ptr, INT_64);
  buf->pos += INT_64;
}

void perl_mongo_serialize_double(buffer *buf, double num) {
  int i;
  double *ptr = &num;

  if(BUF_REMAINING <= DOUBLE_64) {
    perl_mongo_resize_buf(buf, DOUBLE_64);
  }

  SERIALIZE(buf->pos, ptr, DOUBLE_64);
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

void perl_mongo_serialize_bindata(buffer *buf, SV *sv)
{
  STRLEN len;
  const char *bytes = SvPVbyte (sv, len);

  // length of length+bindata
  perl_mongo_serialize_int(buf, len+4);
  
  // TODO: type
  perl_mongo_serialize_byte(buf, 2);
  
  // length
  perl_mongo_serialize_int(buf, len);
  // bindata
  perl_mongo_serialize_bytes(buf, bytes, len);
}

void perl_mongo_serialize_key(buffer *buf, const char *str, void *prep) {
  SV *c = get_sv("MongoDB::BSON::char", 0);

  if(BUF_REMAINING <= strlen(str)+1) {
    perl_mongo_resize_buf(buf, strlen(str)+1);
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
  int i;
  int total = buf->pos - start;
  int *ptr = &total;

  SERIALIZE(start, ptr, INT_32);
}

static void append_sv (buffer *buf, const char *key, SV *sv, AV *ids);

/* add an _id */
static void
perl_mongo_prep(buffer *buf, AV *ids) {
  SV *id = perl_mongo_construct_instance (OID_CLASS, NULL);
  append_sv(buf, "_id", id, NO_PREP);
  av_push(ids, id);
}

static void
hv_to_bson (buffer *buf, SV *sv, AV *ids)
{
    int start;
    HE *he;
    HV *hv;

    /* keep a record of the starting position
     * as an offset, in case the memory is resized */
    start = buf->pos-buf->start;

    /* skip first 4 bytes to leave room for size */
    buf->pos += INT_32;

    hv = (HV*)SvRV(sv);

    if (ids) {
      if(hv_exists(hv, "_id", strlen("_id"))) {
        SV **id = hv_fetch(hv, "_id", strlen("_id"), 0);
        append_sv(buf, "_id", *id, NO_PREP);
        SvREFCNT_inc(*id);
        av_push(ids, *id);
      }
      else {
        perl_mongo_prep(buf, ids);
      }
    }


    (void)hv_iterinit (hv);
    while ((he = hv_iternext (hv))) {
        STRLEN len;
        const char *key = HePV (he, len);

        /* if we've already added the oid field, continue */
        if (ids && strcmp(key, "_id") == 0) {
          continue;
        }
        append_sv (buf, key, HeVAL (he), NO_PREP);
    }

    perl_mongo_serialize_null(buf);
    perl_mongo_serialize_size(buf->start+start, buf);
}

static void
av_to_bson (buffer *buf, AV *av)
{
    I32 i;
    int start;

    start = buf->pos-buf->start;
    buf->pos += INT_32;

    for (i = 0; i <= av_len (av); i++) {
        SV **sv;
        SV *key = newSViv (i);
        if (!(sv = av_fetch (av, i, 0)))
            append_sv (buf, SvPVutf8_nolen(key), newSV(0), NO_PREP);
        else
            append_sv (buf, SvPVutf8_nolen(key), *sv, NO_PREP);

        SvREFCNT_dec (key);
    }

    perl_mongo_serialize_null(buf);
    perl_mongo_serialize_size(buf->start+start, buf);
}

static void
ixhash_to_bson(buffer *buf, SV *sv, AV *ids) {
    int start, i;
    SV **keys_sv, **values_sv;
    AV *array, *keys, *values;
    
    /* skip 4 bytes for size */
    start = buf->pos-buf->start;
    buf->pos += INT_32;
    
    /*
     * a Tie::IxHash is of the form:
     * [ {hash}, [keys], [order], 0 ]
     */
    array = (AV*)SvRV(sv);

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
        append_sv(buf, "_id", *id, NO_PREP);
        av_push(ids, *id);
      }
      else {
        perl_mongo_prep(buf, ids);
      }
    }
    
    for (i=0; i<=av_len(keys); i++) {
        SV **k, **v;
        if (!(k = av_fetch(keys, i, 0)) ||
            !(v = av_fetch(values, i, 0))) {
            croak ("failed to fetch associative array value");
        }
        append_sv(buf, SvPVutf8_nolen(*k), *v, NO_PREP);
    }

    perl_mongo_serialize_null(buf);
    perl_mongo_serialize_size(buf->start+start, buf);
}

static void
append_sv (buffer *buf, const char *key, SV *sv, AV *ids)
{
    if (!SvOK(sv)) {
        set_type(buf, BSON_NULL);
        perl_mongo_serialize_key(buf, key, ids);
        return;
    }
    if (SvROK (sv)) {
        if (sv_isobject (sv)) {
            /* OIDs */
            if (sv_derived_from (sv, OID_CLASS)) {
                SV *attr = perl_mongo_call_reader (sv, "value");
                char *str = SvPV_nolen (attr);

                set_type(buf, BSON_OID);
                perl_mongo_serialize_key(buf, key, ids);
                perl_mongo_serialize_oid(buf, str);

                SvREFCNT_dec (attr);
            }
	    /* Tie::IxHash */
            else if (sv_isa(sv, "Tie::IxHash")) {
              set_type(buf, BSON_OBJECT);
              perl_mongo_serialize_key(buf, key, ids);
              ixhash_to_bson(buf, sv, NO_PREP);
            }
	    /* DateTime */
            else if (sv_isa(sv, "DateTime")) {
              SV *sec, *ms;
              set_type(buf, BSON_DATE);
              perl_mongo_serialize_key(buf, key, ids);
              sec = perl_mongo_call_reader (sv, "epoch");
              ms = perl_mongo_call_method (sv, "millisecond", 0);

              perl_mongo_serialize_long(buf, (int64_t)SvIV(sec)*1000+SvIV(ms));

              SvREFCNT_dec (sec);
              SvREFCNT_dec (ms);
            }
	    /* boolean */
            else if (sv_isa(sv, "boolean")) {
              set_type(buf, BSON_BOOL);
              perl_mongo_serialize_key(buf, key, ids);
              perl_mongo_serialize_byte(buf, SvIV(SvRV(sv)));
            }
            else if (sv_isa(sv, "MongoDB::MinKey")) {
              set_type(buf, BSON_MINKEY);
              perl_mongo_serialize_key(buf, key, ids);
            }
            else if (sv_isa(sv, "MongoDB::MaxKey")) {
              set_type(buf, BSON_MAXKEY);
              perl_mongo_serialize_key(buf, key, ids);
            }
            else if (SvTYPE(SvRV(sv)) == SVt_PVMG) {
              MAGIC *remg;

              if (remg = mg_find((SV*)SvRV(sv), PERL_MAGIC_qr)) {
		/* regular expression */
                int f=0, i=0;
                STRLEN string_length;
                char flags[] = {0,0,0,0,0,0};
                char *string;
                REGEXP *re = (REGEXP *) remg->mg_obj;

                set_type(buf, BSON_REGEX);
                perl_mongo_serialize_key(buf, key, ids);
                perl_mongo_serialize_string(buf, re->precomp, re->prelen);

                string = SvPV(sv, string_length);
                
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
              else {
		/* binary */
                set_type(buf, BSON_BINARY);
                perl_mongo_serialize_key(buf, key, ids);
                perl_mongo_serialize_bindata(buf, SvRV(sv));
              }
            }
        } else {
            switch (SvTYPE (SvRV (sv))) {
                case SVt_PVHV:
                    /* hash */
                    set_type(buf, BSON_OBJECT);
                    perl_mongo_serialize_key(buf, key, ids);
                    /* don't add a _id to inner objs */
                    hv_to_bson (buf, sv, NO_PREP);
                    break;
                case SVt_PVAV:
                    /* array */
                    set_type(buf, BSON_ARRAY);
                    perl_mongo_serialize_key(buf, key, ids);
                    av_to_bson (buf, (AV *)SvRV (sv));
                    break;
                default:
                    sv_dump(SvRV(sv));
                    croak ("type (ref) unhandled");
            }
        }
    } else {
        switch (SvTYPE (sv)) {
	    /* double */
            case SVt_NV: 
            case SVt_PVNV: {
              if (SvNOK(sv)) {
                set_type(buf, BSON_DOUBLE);
                perl_mongo_serialize_key(buf, key, ids);
                perl_mongo_serialize_double(buf, (double)SvNV (sv));
                break;
              }
            }
            /* int */
            case SVt_IV:
            case SVt_PVIV: {
              if (SvIOK(sv)) {
                set_type(buf, BSON_INT);
                perl_mongo_serialize_key(buf, key, ids);
                perl_mongo_serialize_int(buf, (int)SvIV (sv));
                break;
              }
            }
	    /* string */
            case SVt_PV:
            case SVt_PVMG:
                /* Do we need SVt_PVLV here, too? */
                if (sv_len (sv) != strlen (SvPV_nolen (sv))) {
                    set_type(buf, BSON_BINARY);
                    perl_mongo_serialize_key(buf, key, ids);
                    perl_mongo_serialize_bindata(buf, sv);
                }
                else {
                    STRLEN len;
                    const char *str = SvPVutf8(sv, len);

                    set_type(buf, BSON_STRING);
                    perl_mongo_serialize_key(buf, key, ids);
                    perl_mongo_serialize_int(buf, len+1);
                    perl_mongo_serialize_string(buf, str, len);
                }
                break;
            default:
                sv_dump(sv);
                croak ("type (sv) unhandled");
        }
    }
}

void
perl_mongo_sv_to_bson (buffer *buf, SV *sv, AV *ids)
{
    if (!SvROK (sv)) {
        croak ("not a reference");
    }

    switch (SvTYPE (SvRV (sv))) {
        case SVt_PVHV:
            hv_to_bson (buf, sv, ids);
            break;
        case SVt_PVAV: {
            /* TODO: figure out why this segfaults if given "Tie::Hash" */
            if (sv_isa(sv, "Tie::IxHash")) {
                ixhash_to_bson(buf, sv, ids);
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
                            append_sv(buf, "_id", *val, NO_PREP);
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
                    if ( !((key = av_fetch (av, i, 0)) && (val = av_fetch (av, i + 1, 0))) ) {
                        croak ("failed to fetch array element");
                    }
                    append_sv (buf, SvPVutf8_nolen (*key), *val, NO_PREP);
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
