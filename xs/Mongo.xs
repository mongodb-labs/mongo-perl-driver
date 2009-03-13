#include "perl_mongo.h"

extern "C" XS(boot_MongoDB__Connection);
extern "C" XS(boot_MongoDB__Cursor);
extern "C" XS(boot_MongoDB__OID);

MODULE = MongoDB  PACKAGE = MongoDB

PROTOTYPES: DISABLE

BOOT:
	PERL_MONGO_CALL_BOOT (boot_MongoDB__Connection);
	PERL_MONGO_CALL_BOOT (boot_MongoDB__Cursor);
	PERL_MONGO_CALL_BOOT (boot_MongoDB__OID);
