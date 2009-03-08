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
        SV *cursor_class, *oid_class;
    INIT:
        cursor_class = perl_mongo_call_reader (ST (0), "_cursor_class");
        oid_class = perl_mongo_call_reader (ST (0), "_oid_class");
        q = new mongo::Query(perl_mongo_hv_to_bson (query, SvPV_nolen (oid_class)));
    CODE:
        cursor = THIS->query(ns, *q, limit, skip);
        RETVAL = perl_mongo_construct_instance_with_magic (SvPV_nolen (cursor_class), cursor.release(), "_oid_class", oid_class, NULL);
    OUTPUT:
        RETVAL
    CLEANUP:
        SvREFCNT_dec (cursor_class);
        SvREFCNT_dec (oid_class);

SV *
mongo::DBClientConnection::_find_one (ns, query)
        const char *ns
        HV *query
    PREINIT:
        SV *attr;
        mongo::Query *q;
        mongo::BSONObj ret;
    INIT:
        attr = perl_mongo_call_reader (ST (0), "_oid_class");
        q = new mongo::Query(perl_mongo_hv_to_bson (query, SvPV_nolen (attr)));
    CODE:
        ret = THIS->findOne(ns, *q);
        RETVAL = perl_mongo_bson_to_sv (SvPV_nolen (attr), ret);
    OUTPUT:
        RETVAL
    CLEANUP:
        SvREFCNT_dec (attr);

void
mongo::DBClientConnection::_insert (ns, object)
        const char *ns
        HV *object
    PREINIT:
        SV *oid_class;
    INIT:
        oid_class = perl_mongo_call_reader (ST (0), "_oid_class");
    CODE:
        THIS->insert(ns, perl_mongo_hv_to_bson (object, SvPV_nolen (oid_class)));
    CLEANUP:
        SvREFCNT_dec (oid_class);

void
mongo::DBClientConnection::_remove (ns, query, just_one)
        const char *ns
        HV *query
        bool just_one
    PREINIT:
        SV *oid_class;
        mongo::Query *q;
    INIT:
        oid_class = perl_mongo_call_reader (ST (0), "_oid_class");
        q = new mongo::Query(perl_mongo_hv_to_bson (query, SvPV_nolen (oid_class)));
    CODE:
        THIS->remove(ns, *q, just_one);
    CLEANUP:
        SvREFCNT_dec (oid_class);

void
mongo::DBClientConnection::_update (ns, query, object, upsert)
        const char *ns
        HV *query
        HV *object
        bool upsert
    PREINIT:
        SV *oid_class;
        mongo::Query *q;
    INIT:
        oid_class = perl_mongo_call_reader (ST (0), "_oid_class");
        q = new mongo::Query(perl_mongo_hv_to_bson (query, SvPV_nolen (oid_class)));
    CODE:
        THIS->update(ns, *q, perl_mongo_hv_to_bson (object, SvPV_nolen (oid_class)), upsert);
    CLEANUP:
        SvREFCNT_dec (oid_class);
