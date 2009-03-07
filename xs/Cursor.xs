#include "perl_mongo.h"

MODULE = Mongo::Cursor  PACKAGE = Mongo::Cursor

PROTOTYPES: DISABLE

bool
mongo::DBClientCursor::_more ()
    CODE:
        RETVAL = THIS->more();
    OUTPUT:
        RETVAL

SV *
mongo::DBClientCursor::_next ()
    CODE:
        mongo::BSONObj obj = THIS->next();
        RETVAL = perl_mongo_bson_to_sv (obj);
    OUTPUT:
        RETVAL

void
mongo::DBClientCursor::DESTROY ()
