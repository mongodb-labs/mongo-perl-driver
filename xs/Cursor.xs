#include "perl_mongo.h"

MODULE = MongoDB::Cursor  PACKAGE = MongoDB::Cursor

PROTOTYPES: DISABLE

bool
mongo::DBClientCursor::_more ()
    CODE:
        RETVAL = THIS->more();
    OUTPUT:
        RETVAL

SV *
mongo::DBClientCursor::_next ()
    PREINIT:
        SV *attr;
    INIT:
        attr = perl_mongo_call_reader (ST (0), "_oid_class");
    CODE:
        mongo::BSONObj obj = THIS->next();
        RETVAL = perl_mongo_bson_to_sv (SvPV_nolen (attr), obj);
    OUTPUT:
        RETVAL
    CLEANUP:
        SvREFCNT_dec (attr);

void
mongo::DBClientCursor::DESTROY ()
