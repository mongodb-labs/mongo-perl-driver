#include "perl_mongo.h"

MODULE = Mongo::Connection  PACKAGE = Mongo::Connection

PROTOTYPES: DISABLE

void
_build_xs (self)
		SV *self
	PREINIT:
		mongo::DBClientConnection *conn;
		SV *attr;
		bool auto_reconnect;
	INIT:
		attr = perl_mongo_call_reader (self, "auto_reconnect");
		auto_reconnect = SvTRUE (attr);
	CODE:
		conn = new mongo::DBClientConnection (auto_reconnect);
		perl_mongo_attach_ptr_to_instance (self, (void *)conn);
	CLEANUP:
		SvREFCNT_dec (attr);
