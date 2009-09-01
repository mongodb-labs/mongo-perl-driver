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

static SV *bson_to_av (const char *oid_class, buffer *buf);

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
oid_to_sv (const char *oid_class, buffer *buf)
{
    char *id;
    Newxz(id, 25, char);
    perl_mongo_oid_create(buf->pos, id);
    return perl_mongo_construct_instance (oid_class, "value", newSVpvn (id, 24), NULL);
}

static SV *
elem_to_sv (const char *oid_class, int type, buffer *buf)
{
  SV *value;

  switch(type) {
  case BSON_OID: {
    value = oid_to_sv(oid_class, buf);
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

    // len includes \0
    Newx(str, len, char);
    memcpy(str, buf->pos, len);
    value = newSVpvn(str, len-1);
    buf->pos += len;
    break;
  }
  case BSON_OBJECT: {
    value = perl_mongo_bson_to_sv(oid_class, buf);
    break;
  }
  case BSON_ARRAY: {
    value = bson_to_av(oid_class, buf);
    break;
  }
  case BSON_BINARY: {
    int len = *(int*)buf->pos;
    char type, *bytes;

    buf->pos += INT_32;

    type = *buf->pos++;

    Newxz(bytes, len+1, char);
    memcpy(bytes, buf->pos, len);
    buf->pos += len;

    value = newSVpvn(bytes, len);
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
  case BSON_LONG: 
  case BSON_DATE: {
    value = newSViv(*((long long int*)buf->pos));
    buf->pos += INT_64;
    break;
  }
  case BSON_REGEX: {
    // TODO
    /*char *regex, *flags;
      int regex_len, flags_len;

      regex = buf->pos;
      regex_len = strlen(buf->pos);
      buf->pos += regex_len+1;

      flags = buf->pos;
      flags_len = strlen(buf->pos);
      buf->pos += flags_len+1;

      object_init_ex(value, mongo_ce_Regex);

      add_property_stringl(value, "regex", regex, regex_len, 1);
      add_property_stringl(value, "flags", flags, flags_len, 1);
    */
    break;
  }
  case BSON_CODE: 
  case BSON_CODE__D: {
    // TODO
    /*zval *zcope;
      int code_len;
      char *code;

      object_init_ex(value, mongo_ce_Code);
      // initialize scope array
      MAKE_STD_ZVAL(zcope);
      array_init(zcope);

      // CODE has a useless total size field
      if (type == BSON_CODE) {
      buf->pos += INT_32;
      }

      // length of code (includes \0)
      code_len = *(int*)buf->pos;
      buf->pos += INT_32;

      code = buf->pos;
      buf->pos += code_len;

      if (type == BSON_CODE) {
      buf->pos = bson_to_zval(buf->pos, HASH_P(zcope) TSRMLS_CC);
      }

      // exclude \0
      add_property_stringl(value, "code", code, code_len-1, DUP);
      add_property_zval(value, "scope", zcope);

      // somehow, we pick up an extra zcope ref
      zval_ptr_dtor(&zcope);
    */
    break;
  }
  case BSON_TIMESTAMP: {
    value = newSViv((long)*(int*)buf->pos);
    buf->pos += INT_64;
    break;
  }
  case BSON_MINKEY: {
    STRLEN len;
    value = newSVpv("[MinKey]", len);
    break;
  }
  case BSON_MAXKEY: {
    STRLEN len;
    value = newSVpv("[MaxKey]", len);
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
bson_to_av (const char *oid_class, buffer *buf)
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
      if ((sv = elem_to_sv (oid_class, type, buf))) {
        av_push (ret, sv);
      }
    }

    return newRV_noinc ((SV *)ret);
}

SV *
perl_mongo_bson_to_sv (const char *oid_class, buffer *buf)
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
      value = elem_to_sv(oid_class, type, buf);
      if (!hv_store (ret, name, strlen (name), value, 0)) {
        croak ("failed storing value in hash");
      }
    }

    return newRV_noinc ((SV *)ret);
}

static int resize_buf(buffer *buf, int size) {
  int total = buf->end - buf->start;
  int used = buf->pos - buf->start;

  total = total < GROW_SLOWLY ? total*2 : total+INITIAL_BUF_SIZE;
  while (total-used < size) {
    total += size;
  }

  buf->start = (char*)realloc(buf->start, total);
  buf->pos = buf->start + used;
  buf->end = buf->start + total;
  return total;
}

inline void serialize_byte(buffer *buf, char b) {
  if(BUF_REMAINING <= 1) {
    resize_buf(buf, 1);
  }
  *(buf->pos) = b;
  buf->pos += 1;
}

inline void serialize_bytes(buffer *buf, const char *str, int str_len) {
  if(BUF_REMAINING <= str_len) {
    resize_buf(buf, str_len);
  }
  memcpy(buf->pos, str, str_len);
  buf->pos += str_len;
}

inline void serialize_string(buffer *buf, const char *str, int str_len) {
  if(BUF_REMAINING <= str_len+1) {
    resize_buf(buf, str_len+1);
  }

  memcpy(buf->pos, str, str_len);
  // add \0 at the end of the string
  buf->pos[str_len] = 0;
  buf->pos += str_len + 1;
}

inline void serialize_int(buffer *buf, int num) {
  if(BUF_REMAINING <= INT_32) {
    resize_buf(buf, INT_32);
  }
  memcpy(buf->pos, &num, INT_32);
  buf->pos += INT_32;
}

inline void serialize_long(buffer *buf, long long num) {
  if(BUF_REMAINING <= INT_64) {
    resize_buf(buf, INT_64);
  }
  memcpy(buf->pos, &num, INT_64);
  buf->pos += INT_64;
}

inline void serialize_double(buffer *buf, double num) {
  if(BUF_REMAINING <= INT_64) {
    resize_buf(buf, INT_64);
  }
  memcpy(buf->pos, &num, DOUBLE_64);
  buf->pos += DOUBLE_64;
}

inline void serialize_oid(buffer *buf, char *id) {
  int i;

  if(BUF_REMAINING <= OID_SIZE) {
    resize_buf(buf, OID_SIZE);
  }

  for(i=0;i<OID_SIZE;i++) {
    char digit1 = id[i*2], digit2 = id[i*2+1];
    digit1 = digit1 >= 'a' && digit1 <= 'f' ? digit1 -= 87 : digit1;
    digit1 = digit1 >= 'A' && digit1 <= 'F' ? digit1 -= 55 : digit1;
    digit1 = digit1 >= '0' && digit1 <= '9' ? digit1 -= 48 : digit1;
    
    digit2 = digit2 >= 'a' && digit2 <= 'f' ? digit2 -= 87 : digit2;
    digit2 = digit2 >= 'A' && digit2 <= 'F' ? digit2 -= 55 : digit2;
    digit2 = digit2 >= '0' && digit2 <= '9' ? digit2 -= 48 : digit2;
    
    buf->pos[i] = digit1*16+digit2;
  }
  buf->pos += OID_SIZE;
}

/* the position is not increased, we are just filling
 * in the first 4 bytes with the size.
 */
void serialize_size(char *start, buffer *buf) {
  int total = buf->pos - start;
  memcpy(start, &total, INT_32);
}

static void append_sv (buffer *buf, const char *key, SV *sv);

/* add an _id */
static void
prep(buffer *buf, SV *sv) {
  HV *hash = (HV*)SvRV(sv);
  if (hv_exists(hash, "_id", strlen("_id"))) {
    SV **id = hv_fetch(hash, "_id", strlen("_id"), 0);
    append_sv(buf, "_id", *id);
  }
}

static void
hv_to_bson (buffer *buf, SV *sv, int add_oid)
{
    int start;
    HE *he;
    HV *hv;

    /* keep a record of the starting position
     * as an offset, in case the memory is resized */
    start = buf->pos-buf->start;

    /* skip first 4 bytes to leave room for size */
    buf->pos += INT_32;

    if (add_oid) {
      prep(buf, sv);
    }

    hv = (HV*)SvRV(sv);

    (void)hv_iterinit (hv);
    while ((he = hv_iternext (hv))) {
        STRLEN len;
        const char *key = HePV (he, len);

        /* if we've already added the oid field, continue */
        if (add_oid && strcmp(key, "_id") == 0) {
          continue;
        }
        append_sv (buf, key, HeVAL (he)); 
    }

    serialize_null(buf);
    serialize_size(buf->start+start, buf);
}

static void
av_to_bson (buffer *buf, AV *av)
{
    int start;

    start = buf->pos-buf->start;
    buf->pos += INT_32;

    I32 i;
    for (i = 0; i <= av_len (av); i++) {
        SV **sv;
        SV *key = newSViv (i);
        if (!(sv = av_fetch (av, i, 0))) {
            croak ("failed to fetch array value");
        }
        append_sv (buf, SvPVutf8_nolen(key), *sv);
        SvREFCNT_dec (key);
    }

    serialize_null(buf);
    serialize_size(buf->start+start, buf);
}

static void
append_sv (buffer *buf, const char *key, SV *sv)
{
    if (!SvOK(sv)) {
        set_type(buf, BSON_NULL);
        serialize_string(buf, key, strlen(key));
        return;
    }
    if (SvROK (sv)) {
        if (sv_isobject (sv)) {
            if (sv_derived_from (sv, OID_CLASS)) {
                SV *attr = perl_mongo_call_reader (sv, "value");
                char *str = SvPV_nolen (attr);

                set_type(buf, BSON_OID);
                serialize_string(buf, key, strlen(key));
                serialize_oid(buf, str);

                SvREFCNT_dec (attr);
            }
            else if (sv_derived_from (sv, "Tie::Hash")) {
              int start, i;
              SV **keys_sv, **values_sv;
              AV *array, *keys, *values;

              array = (AV*)SvRV(sv);

              // keys
              keys_sv = av_fetch(array, 1, 0);
              keys = (AV*)SvRV(*keys_sv);
              values_sv = av_fetch(array, 2, 0);
              values = (AV*)SvRV(*values_sv);

              set_type(buf, BSON_OBJECT);
              serialize_string(buf, key, strlen(key));

              start = buf->pos-buf->start;
              buf->pos += INT_32;

              for (i=0; i<=av_len(keys); i++) {
                SV **k, **v;
                if (!(k = av_fetch(keys, i, 0)) ||
                    !(v = av_fetch(values, i, 0))) {
                  croak ("failed to fetch associative array value");
                }
                append_sv(buf, SvPVutf8_nolen(*k), *v);
              }

              serialize_null(buf);
              serialize_size(buf->start+start, buf);
            }
            else if (SvTYPE(SvRV(sv)) == SVt_PVMG) {
              int f=0;
              char flags[] = {0,0,0,0,0,0};
              REGEXP *re = SvRX(sv);
              if (!re) {
                croak ("couldn't parse this type of obj");
              }

              set_type(buf, BSON_REGEX);
              serialize_string(buf, key, strlen(key));
              serialize_string(buf, re->precomp, re->prelen);

              if (re->extflags & RXf_PMf_FOLD) 
                flags[f++] = 'i';
              if (re->extflags & RXf_PMf_MULTILINE)
                flags[f++] = 'm';
              if (re->extflags & RXf_PMf_EXTENDED)
                flags[f++] = 'x';
              if (re->extflags & RXf_PMf_LOCALE)
                flags[f++] = 'l';
              if (re->extflags & RXf_PMf_SINGLELINE)
                flags[f++] = 's';
              serialize_string(buf, flags, strlen(flags));
            }
        } else {
            switch (SvTYPE (SvRV (sv))) {
                case SVt_PVHV:
                    set_type(buf, BSON_OBJECT);
                    serialize_string(buf, key, strlen(key));
                    /* don't add a _id to inner objs */
                    hv_to_bson (buf, sv, NO_PREP);
                    break;
                case SVt_PVAV:
                    set_type(buf, BSON_ARRAY);
                    serialize_string(buf, key, strlen(key));
                    av_to_bson (buf, (AV *)SvRV (sv));
                    break;
                default:
                    sv_dump(SvRV(sv));
                    croak ("type (ref) unhandled");
            }
        }
    } else {
        switch (SvTYPE (sv)) {
            case SVt_IV:
                set_type(buf, BSON_INT);
                serialize_string(buf, key, strlen(key));
                serialize_int(buf, (int)SvIV (sv));
                break;
            // stupid special casing for bools
            case SVt_PVNV: {
              if (sv == &PL_sv_yes) {
                set_type(buf, BSON_BOOL);
                serialize_string(buf, key, strlen(key));
                serialize_byte(buf, 1);
                break;
              }
              else if (sv == &PL_sv_no) {
                set_type(buf, BSON_BOOL);
                serialize_string(buf, key, strlen(key));
                serialize_byte(buf, 0);
                break;
              }
              // otherwise, continue
            }
            case SVt_PV:
            case SVt_NV:
            case SVt_PVIV:
            case SVt_PVMG:
                /* Do we need SVt_PVLV here, too? */
                if (sv_len (sv) != strlen (SvPV_nolen (sv))) {
                    STRLEN len;
                    const char *bytes = SvPVbyte (sv, len);

                    set_type(buf, BSON_BINARY);
                    serialize_string(buf, key, strlen(key));
                    serialize_int(buf, len);
		    // TODO: replace with something
                    serialize_byte(buf, 2);
                    serialize_bytes(buf, bytes, len);
                }
                else {
                    STRLEN len;
                    const char *str = SvPVutf8(sv, len);

                    set_type(buf, BSON_STRING);
                    serialize_string(buf, key, strlen(key));
                    serialize_int(buf, len+1);
                    serialize_string(buf, str, len);
                }
                break;
            default:
                sv_dump(sv);
                croak ("type (sv) unhandled");
        }
    }
}

void
perl_mongo_sv_to_bson (buffer *buf, SV *sv, int add_oid)
{
    if (!SvROK (sv)) {
        croak ("not a reference");
    }

    switch (SvTYPE (SvRV (sv))) {
        case SVt_PVHV:
            hv_to_bson (buf, sv, add_oid);
            break;
        case SVt_PVAV: {
            I32 i;
            AV *av = (AV *)SvRV (sv);
            if ((av_len (av) % 2) == 0) {
                croak ("odd number of elements in structure");
            }

            int start;
            
            start = buf->pos-buf->start;
            buf->pos += INT_32;

            /* 
             * we don't need to worry about serializing the _id,
             * as it's illegal to insert an array 
             */

            for (i = 0; i <= av_len (av); i += 2) {
                SV **key, **val;
                if ( !((key = av_fetch (av, i, 0)) && (val = av_fetch (av, i + 1, 0))) ) {
                    croak ("failed to fetch array element");
                }
                append_sv (buf, SvPVutf8_nolen (*key), *val);
            }

            serialize_null(buf);
            serialize_size(buf->start+start, buf);

            break;
        }
        default:
            sv_dump(sv);
            croak ("type unhandled");
    }
}
