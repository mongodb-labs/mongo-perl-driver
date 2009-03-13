#include "perl_mongo.h"

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
    HV *ret = newHV ();

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

static void append_sv (mongo::BSONObjBuilder *builder, const char *key, SV *sv, const char *oid_class);

static void
hv_to_bson (mongo::BSONObjBuilder *builder, HV *hv, const char *oid_class)
{
    HE *he;
    (void)hv_iterinit (hv);
    while ((he = hv_iternext (hv))) {
        STRLEN len;
        const char *key = HePV (he, len);
        append_sv (builder, key, HeVAL (he), oid_class);
    }
}

static void
av_to_bson (mongo::BSONObjBuilder *builder, AV *av, const char *oid_class)
{
    I32 i;
    for (i = 0; i <= av_len (av); i++) {
        SV **sv;
        SV *key = newSViv (i);
        if (!(sv = av_fetch (av, i, 0))) {
            croak ("failed to fetch array value");
        }
        append_sv (builder, SvPV_nolen(key), *sv, oid_class);
        SvREFCNT_dec (key);
    }
}

static void
append_sv (mongo::BSONObjBuilder *builder, const char *key, SV *sv, const char *oid_class)
{
    switch (SvTYPE (sv)) {
        case SVt_IV:
            builder->append(key, (int)SvIV (sv));
            break;
        case SVt_PV:
            builder->append(key, (char *)SvPV_nolen (sv));
            break;
        case SVt_RV: {
            mongo::BSONObjBuilder *subobj = new mongo::BSONObjBuilder();
            if (sv_isobject (sv)) {
                if (sv_derived_from (sv, oid_class)) {
                    SV *attr = perl_mongo_call_reader (sv, "value");
                    std::string *str = new string(SvPV_nolen (attr));
                    mongo::OID *id = new mongo::OID();
                    id->init(*str);
                    builder->appendOID(key, id);
                    SvREFCNT_dec (attr);
                }
            } else {
                switch (SvTYPE (SvRV (sv))) {
                    case SVt_PVHV:
                        hv_to_bson (subobj, (HV *)SvRV (sv), oid_class);
                        break;
                    case SVt_PVAV:
                        av_to_bson (subobj, (AV *)SvRV (sv), oid_class);
                        break;
                    default:
                        croak ("type unhandled");
                }
                builder->append(key, subobj->done());
            }
            break;
        }
        default:
            croak ("type unhandled");
    }
}

mongo::BSONObj
perl_mongo_sv_to_bson (SV *sv, const char *oid_class)
{
    mongo::BSONObjBuilder *builder = new mongo::BSONObjBuilder();

    if (!SvROK (sv)) {
        croak ("not a reference");
    }

    switch (SvTYPE (SvRV (sv))) {
        case SVt_PVHV:
            hv_to_bson (builder, (HV *)SvRV (sv), oid_class);
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
                append_sv (builder, SvPV_nolen (*key), *val, oid_class);
            }

            break;
        }
        default:
            croak ("type unhandled");
    }

    mongo::BSONObj obj = builder->done();
    return obj;
}
