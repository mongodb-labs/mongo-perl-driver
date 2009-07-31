#include "perl_mongo.h"

MODULE = MongoDB::OID  PACKAGE = MongoDB::OID

PROTOTYPES: DISABLE

SV *
_build_value (self, c_str)
        SV *self
        const char *c_str;
    CODE:
        mongo::OID oid;
        if (c_str && strlen(c_str) == 24) {
           oid.init(string(c_str));
        } else {
           oid.init();
        }
        RETVAL = newSVpv (oid.str().c_str(), oid.str().length());
    OUTPUT:
        RETVAL


        