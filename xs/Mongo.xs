#include "perl_mongo.h"

extern "C" XS(boot_Mongo__Connection);
extern "C" XS(boot_Mongo__Cursor);
extern "C" XS(boot_Mongo__OID);

MODULE = Mongo  PACKAGE = Mongo

PROTOTYPES: DISABLE

BOOT:
	PERL_MONGO_CALL_BOOT (boot_Mongo__Connection);
	PERL_MONGO_CALL_BOOT (boot_Mongo__Cursor);
	PERL_MONGO_CALL_BOOT (boot_Mongo__OID);
