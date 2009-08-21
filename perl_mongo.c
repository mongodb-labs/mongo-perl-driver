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
perl_mongo_call_writer (SV *self, const char *reader, SV *value)
{
    dSP;
    SV *ret;
    I32 count;

    ENTER;
    SAVETMPS;

    PUSHMARK (SP);
    XPUSHs (self);
    XPUSHs (value);
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

static SV *bson_to_av (const char *oid_class, mongo::BSONObj obj);

static SV *
oid_to_sv (const char *oid_class, mongo::OID id)
{
    std::string str = id.str();
    return perl_mongo_construct_instance (oid_class, "value", newSVpv (str.c_str(), str.length()), NULL);
}

static SV *
elem_to_sv (const char *oid_class, mongo::BSONElement elem)
{
    switch (elem.type()) {
        case mongo::Undefined:
        case mongo::jstNULL:
            return &PL_sv_undef;
            break;
        case mongo::NumberInt:
            return newSViv (elem.number());
            break;
        case mongo::NumberDouble:
            return newSVnv (elem.number());
            break;
        case mongo::Bool:
            return elem.boolean() ? &PL_sv_yes : &PL_sv_no;
            break;
        case mongo::String:
            return newSVpv (elem.valuestr(), 0);
            break;
        case mongo::Array:
            return bson_to_av (oid_class, elem.embeddedObject());
            break;
        case mongo::Object:
            return perl_mongo_bson_to_sv (oid_class, elem.embeddedObject());
            break;
        case mongo::jstOID:
            return oid_to_sv (oid_class, elem.__oid());
            break;
        case mongo::EOO:
            return NULL;
            break;
        case mongo::BinData: {
            const char *data;
            int len;
            if (elem.binDataType() != mongo::ByteArray) {
                croak ("bindata type unhandled");
            }
            data = elem.binData(len);
            return newSVpv (data, len);
            break;
        }
        default:
            croak ("type unhandled");
    }
}

static SV *
bson_to_av (const char *oid_class, mongo::BSONObj obj)
{
    AV *ret = newAV ();
    mongo::BSONObjIterator it = mongo::BSONObjIterator(obj);
    while (it.more()) {
        SV *sv;
        mongo::BSONElement elem = it.next();
        if ((sv = elem_to_sv (oid_class, elem))) {
            av_push (ret, sv);
        }
    }

    return newRV_noinc ((SV *)ret);
}

SV *
perl_mongo_bson_to_sv (const char *oid_class, mongo::BSONObj obj)
{
    HV *ret;

    if (obj.isEmpty()) {
        return &PL_sv_undef;
    }

    ret  = newHV ();
    mongo::BSONObjIterator it = mongo::BSONObjIterator(obj);
    while (it.more()) {
        SV *sv;
        mongo::BSONElement elem = it.next();
        if ((sv = elem_to_sv (oid_class, elem))) {
            const char *key = elem.fieldName();
            if (!hv_store (ret, key, strlen (key), sv, 0)) {
                croak ("failed storing value in hash");
            }
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

/* the position is not increased, we are just filling
 * in the first 4 bytes with the size.
 */
void serialize_size(char *start, buffer *buf) {
  int total = buf->pos - start;
  memcpy(start, &total, INT_32);
}


static void append_sv (buffer *buf, const char *key, SV *sv, const char *oid_class);

static void
hv_to_bson (buffer *buf, HV *hv, const char *oid_class)
{
    HE *he;
    (void)hv_iterinit (hv);
    while ((he = hv_iternext (hv))) {
        STRLEN len;
        const char *key = HePV (he, len);
        append_sv (buf, key, HeVAL (he), oid_class);
    }
}

static void
av_to_bson (buffer *buf, AV *av, const char *oid_class)
{
    I32 i;
    for (i = 0; i <= av_len (av); i++) {
        SV **sv;
        SV *key = newSViv (i);
        if (!(sv = av_fetch (av, i, 0))) {
            croak ("failed to fetch array value");
        }
        append_sv (buf, SvPVutf8_nolen(key), *sv, oid_class);
        SvREFCNT_dec (key);
    }
}

static void
append_sv (buffer *buf, const char *key, SV *sv, const char *oid_class)
{
    if (!SvOK(sv)) {
        set_type(buf, BSON_NULL);
        serialize_string(buf, key, strlen(key));
        return;
    }
    if (SvROK (sv)) {
        if (sv_isobject (sv)) {
            if (sv_derived_from (sv, oid_class)) {
                SV *attr = perl_mongo_call_reader (sv, "value");
                char *str = SvPV_nolen (attr);

                set_type(buf, BSON_OID);
                serialize_string(buf, key, strlen(key));
                serialize_bytes(buf, str, OID_SIZE);

                SvREFCNT_dec (attr);
            }
        } else {
            switch (SvTYPE (SvRV (sv))) {
                case SVt_PVHV:
                    set_type(buf, BSON_OBJECT);
                    serialize_string(buf, key, strlen(key));
                    hv_to_bson (buf, (HV *)SvRV (sv), oid_class);
                    break;
                case SVt_PVAV:
                    set_type(buf, BSON_ARRAY);
                    serialize_string(buf, key, strlen(key));
                    av_to_bson (buf, (AV *)SvRV (sv), oid_class);
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
            case SVt_PV:
            case SVt_NV:
            case SVt_PVIV:
            case SVt_PVMG:
            case SVt_PVNV:
                /* Do we need SVt_PVLV here, too? */
                if (sv_len (sv) != strlen (SvPV_nolen (sv))) {
                    STRLEN len;
                    const char *bytes = SvPVbyte (sv, len);

                    set_type(buf, BSON_BINARY);
                    serialize_string(buf, key, strlen(key));
                    serialize_int(buf, len);
                    serialize_byte(buf, mongo::ByteArray);
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
perl_mongo_sv_to_bson (buffer *buf, SV *sv, const char *oid_class)
{
    int start;

    if (!SvROK (sv)) {
        croak ("not a reference");
    }

    // keep a record of the starting position
    // as an offset, in case the memory is resized
    start = buf->pos-buf->start;

    // skip first 4 bytes to leave room for size
    buf->pos += INT_32;

    switch (SvTYPE (SvRV (sv))) {
        case SVt_PVHV:
            hv_to_bson (buf, (HV *)SvRV (sv), oid_class);
            break;
        case SVt_PVAV: {
            I32 i;
            AV *av = (AV *)SvRV (sv);
            if ((av_len (av) % 2) == 0) {
                croak ("odd number of elements in structure");
            }

            for (i = 0; i <= av_len (av); i += 2) {
                SV **key, **val;
                if ( !((key = av_fetch (av, i, 0)) && (val = av_fetch (av, i + 1, 0))) ) {
                    croak ("failed to fetch array element");
                }
                append_sv (buf, SvPVutf8_nolen (*key), *val, oid_class);
            }

            break;
        }
        default:
            sv_dump(sv);
            croak ("type unhandled");
    }

    serialize_null(buf);
    serialize_size(buf->start+start, buf);
}
