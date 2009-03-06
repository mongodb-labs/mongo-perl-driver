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

void
mongo::DBClientConnection::_connect ()
	PREINIT:
		SV *attr;
		char *server;
		string error;
	INIT:
		attr = perl_mongo_call_reader (ST (0), "_server");
		server = SvPV_nolen (attr);
	CODE:
		if (!THIS->connect(server, error)) {
			croak ("%s", error.c_str());
		}
	CLEANUP:
		SvREFCNT_dec (attr);

SV *
mongo::DBClientConnection::_query (ns, query, limit, skip)
        const char *ns
        HV *query
        int limit
        int skip
    PREINIT:
        std::auto_ptr<mongo::DBClientCursor> cursor;
        mongo::Query *q;
        SV *attr;
    INIT:
        q = new mongo::Query(perl_mongo_hv_to_bson (query));
        attr = perl_mongo_call_reader (ST (0), "_cursor_class");
    CODE:
        cursor = THIS->query(ns, *q, limit, skip);
        RETVAL = perl_mongo_construct_instance_with_magic (SvPV_nolen (attr), cursor.get());
    OUTPUT:
        RETVAL
    CLEANUP:
        SvREFCNT_dec (attr);
