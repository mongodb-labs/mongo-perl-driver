#include "perl_mongo.h"

void
perl_mongo_call_xs (pTHX_ void (*subaddr) (pTHX_ CV *), CV *cv, SV **mark)
{
	dSP;
	PUSHMARK (SP);
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
perl_mongo_construct_instance_with_magic (const char *klass, void *ptr)
{
    dSP;
    SV *ret;
    I32 count;

    ENTER;
    SAVETMPS;

    PUSHMARK (SP);
    mXPUSHp (klass, strlen (klass));
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

    perl_mongo_attach_ptr_to_instance (ret, ptr);

    return ret;
}

static SV *bson_to_av (mongo::BSONObj obj);

static SV *
elem_to_sv (mongo::BSONElement elem)
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
            return bson_to_av (elem.embeddedObject());
            break;
        case mongo::Object:
            return perl_mongo_bson_to_sv (elem.embeddedObject());
            break;
        case mongo::EOO:
            return NULL;
            break;
        default:
            croak ("type unhandled");
    }
}

static SV *
bson_to_av (mongo::BSONObj obj)
{
    AV *ret = newAV ();
    mongo::BSONObjIterator it = mongo::BSONObjIterator(obj);
    while (it.more()) {
        SV *sv;
        mongo::BSONElement elem = it.next();
        if ((sv = elem_to_sv (elem))) {
            av_push (ret, sv);
        }
    }

    return newRV_noinc ((SV *)ret);
}

SV *
perl_mongo_bson_to_sv (mongo::BSONObj obj)
{
    HV *ret = newHV ();

    mongo::BSONObjIterator it = mongo::BSONObjIterator(obj);
    while (it.more()) {
        SV *sv;
        mongo::BSONElement elem = it.next();
        if ((sv = elem_to_sv (elem))) {
            const char *key = elem.fieldName();
            if (!hv_store (ret, key, strlen (key), sv, 0)) {
                croak ("failed storing value in hash");
            }
        }
    }

    return newRV_noinc ((SV *)ret);
}
