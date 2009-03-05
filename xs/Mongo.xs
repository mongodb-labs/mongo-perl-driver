#include "perl_mongo.h"

extern "C" XS(boot_Mongo__Connection);

MODULE = Mongo  PACKAGE = Mongo

PROTOTYPES: DISABLE

BOOT:
	PERL_MONGO_CALL_BOOT (boot_Mongo__Connection);
