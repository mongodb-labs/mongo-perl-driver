#include "perl_mongo.h"
#include "mongo_link.h"

MODULE = MongoDB::Connection  PACKAGE = MongoDB::Connection

PROTOTYPES: DISABLE



void
connect (self)
                SV *self
	PREINIT:
                SV *host_sv, *port_sv;
		char *host;
                int port;
                int socket;
	INIT:
		host_sv = perl_mongo_call_reader (ST (0), "host");
		port_sv = perl_mongo_call_reader (ST (0), "port");
		host = SvPV_nolen(host_sv);
		port = SvIV(port_sv);
	CODE:
                // TODO: pairing
                // this will be be server1, server2 
		if (!(socket = mongo_connect(host, port))) {
                  croak ("could not connect");
                  return;
		}

                this_hash = SvSTASH(SvRV(ST(0)));

                // set the socket
                hv_store(this_hash, "socket", strlen("socket"), newSViv(socket), 0);

	CLEANUP:
		SvREFCNT_dec (host_sv);
		SvREFCNT_dec (port_sv);

SV *
mongo::DBClientConnection::_query (ns, query=0, limit=0, skip=0, sort=0)
        const char *ns
        SV *query
        int limit
        int skip
        SV *sort
    PREINIT:
        HV *this_hash, *stash, *rcursor, *full_query;
    CODE:
        // create a new MongoDB::Cursor
        stash = gv_stashpv("MongoDB::Cursor", 0);
        rcursor = newHV();
        RETVAL = sv_bless(newRV_noinc((SV *)rcursor), stash);

        this_hash = SvSTASH(SvRV(RETVAL));

        // set the connection
        SvREFCNT_inc(ST(0));
        hv_store(this_hash, "connection", strlen("connection"), ST(0), 0);

        // set the namespace
        hv_store(this_hash, "ns", strlen("ns"), newSVpv(ns, strlen(ns)), 0);

        // create the query
        full_query = newHV();
        hv_store(this_hash, "query", strlen("query"), newRV_noinc((SV*)full_query), 0);

        // add the query to the... query
        if (!query || !SvOK(query)) {
          query = newRV_noinc((SV*)newHV());
        }
        hv_store(full_query, "query", strlen("query"), SvREFCNT_inc(query), 0);

        // add sort to the query
        if (sort && SvOK(sort)) {
          hv_store(full_query, "orderby", strlen("orderby"), SvREFCNT_inc(sort), 0);
        }

        // add limit/skip
        hv_store(this_hash, "limit", strlen("limit"), newSViv(limit), 0);
        hv_store(this_hash, "skip", strlen("skip"), newSViv(skip), 0);

    OUTPUT:
        RETVAL


SV *
mongo::DBClientConnection::_find_one (ns, query)
        const char *ns
        SV *query
    PREINIT:
        SV *attr;
        mongo::Query *q;
        mongo::BSONObj ret;
    INIT:
        attr = perl_mongo_call_reader (ST (0), "_oid_class");
        q = new mongo::Query(perl_mongo_sv_to_bson (query, SvPV_nolen (attr)));
    CODE:
        ret = THIS->findOne(ns, *q);
        RETVAL = perl_mongo_bson_to_sv (SvPV_nolen (attr), ret);
    OUTPUT:
        RETVAL
    CLEANUP:
        delete q;
        SvREFCNT_dec (attr);

void
mongo::DBClientConnection::_insert (ns, object)
        const char *ns
        SV *object
    PREINIT:
        SV *oid_class;
    INIT:
        oid_class = perl_mongo_call_reader (ST (0), "_oid_class");
    CODE:
        THIS->insert(ns, perl_mongo_sv_to_bson (object, SvPV_nolen (oid_class)));
    CLEANUP:
        SvREFCNT_dec (oid_class);

void
mongo::DBClientConnection::_remove (ns, query, just_one)
        const char *ns
        SV *query
        bool just_one
    PREINIT:
        SV *oid_class;
        mongo::Query *q;
    INIT:
        oid_class = perl_mongo_call_reader (ST (0), "_oid_class");
        q = new mongo::Query(perl_mongo_sv_to_bson (query, SvPV_nolen (oid_class)));
    CODE:
        THIS->remove(ns, *q, just_one);
    CLEANUP:
        delete q;
        SvREFCNT_dec (oid_class);

void
mongo::DBClientConnection::_update (ns, query, object, upsert)
        const char *ns
        SV *query
        SV *object
        bool upsert
    PREINIT:
        SV *oid_class;
        mongo::Query *q;
    INIT:
        oid_class = perl_mongo_call_reader (ST (0), "_oid_class");
        q = new mongo::Query(perl_mongo_sv_to_bson (query, SvPV_nolen (oid_class)));
    CODE:
        THIS->update(ns, *q, perl_mongo_sv_to_bson (object, SvPV_nolen (oid_class)), upsert);
    CLEANUP:
        delete q;
        SvREFCNT_dec (oid_class);

void
mongo::DBClientConnection::_ensure_index (ns, keys, unique=0)
        const char *ns
        SV *keys
        int unique
    PREINIT:
        SV *oid_class;
        mongo::BSONObj obj;
    INIT:
        oid_class = perl_mongo_call_reader (ST (0), "_oid_class");
        obj = perl_mongo_sv_to_bson (keys, SvPV_nolen (oid_class));
    CODE:
        THIS->ensureIndex(ns, obj, unique);
    CLEANUP:
        SvREFCNT_dec (oid_class);

NO_OUTPUT bool
mongo::DBClientConnection::_authenticate (dbname, username, password, is_digest=false)
        const char *dbname
        const char *username
        const char *password
        bool is_digest
    PREINIT:
        std::string error_message;
        std::string digest_password;
    INIT:
        if (is_digest) {
            digest_password = password;
        } else {
            digest_password = THIS->createPasswordDigest(username, password);
        }
    CODE:
        RETVAL = THIS->auth(dbname, username, password, error_message, true);
    POSTCALL:
        if (!RETVAL) {
            croak ("%s", error_message.c_str());
        }

void
connection_DESTROY (self)
          SV *self
     CODE:
         printf("in destroy\n");
