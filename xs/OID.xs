#include "perl_mongo.h"

MODULE = Mongo::OID  PACKAGE = Mongo::OID

PROTOTYPES: DISABLE

SV *
_build_value (self)
        SV *self
    CODE:
        mongo::OID oid;
        oid.init();
        RETVAL = newSVpv (oid.str().c_str(), oid.str().length());
    OUTPUT:
        RETVAL
