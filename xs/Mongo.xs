#include "perl_mongo.h"

int request_id;

extern XS(boot_MongoDB__Connection);
extern XS(boot_MongoDB__Cursor);
extern XS(boot_MongoDB__OID);

MODULE = MongoDB  PACKAGE = MongoDB

PROTOTYPES: DISABLE

BOOT:
	srand(time(0));
	request_id = rand();
	PERL_MONGO_CALL_BOOT (boot_MongoDB__Connection);
	PERL_MONGO_CALL_BOOT (boot_MongoDB__Cursor);
	PERL_MONGO_CALL_BOOT (boot_MongoDB__OID);
