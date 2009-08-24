#include "perl_mongo.h"
#include "mongo_link.h"

extern int request_id;

MODULE = MongoDB::Connection  PACKAGE = MongoDB::Connection

PROTOTYPES: DISABLE



void
connect (self)
                SV *self
	PREINIT:
                SV *host_sv, *port_sv;
                HV *this_hash;
		char *host;
                int port;
                int socket;
		mongo_link *link;
	INIT:
		host_sv = perl_mongo_call_reader (ST (0), "host");
		port_sv = perl_mongo_call_reader (ST (0), "port");
		host = SvPV_nolen(host_sv);
		port = SvIV(port_sv);
	CODE:
	        Newx(link, 1, mongo_link);
		perl_mongo_attach_ptr_to_instance(self, link);

                // TODO: pairing
                // this will be be server1, server2 
		if (!(socket = mongo_link_connect(host, port))) {
                  croak ("could not connect");
                  return;
		}

		/*link->paired = 0;
		link->server.single.socket = socket;
		link->server.single.host = host;
		link->server.single.port = port;
		*/
                this_hash = SvSTASH(SvRV(ST(0)));

                // set the socket
                hv_store(this_hash, "socket", strlen("socket"), newSViv(socket), 0);

	CLEANUP:
		SvREFCNT_dec (host_sv);
		SvREFCNT_dec (port_sv);

SV *
_query (self, ns, query=0, limit=0, skip=0, sort=0)
        SV *self
        const char *ns
        SV *query
        int limit
        int skip
        SV *sort
    PREINIT:
        mongo_cursor *cursor;
        SV **socket;
        HV *this_hash, *stash, *rcursor, *full_query;
    CODE:
        // create a new MongoDB::Cursor
        stash = gv_stashpv("MongoDB::Cursor", 0);
        rcursor = newHV();
        RETVAL = sv_bless(newRV_noinc((SV *)rcursor), stash);

        // attach a mongo_cursor* to the MongoDB::Cursor
        Newx(cursor, 1, mongo_cursor);
        perl_mongo_attach_ptr_to_instance(RETVAL, cursor);

        // START cursor setup

        // set the connection
        this_hash = SvSTASH(SvRV(self));
        socket = hv_fetch(this_hash, "socket", strlen("socket"), 0);
        cursor->socket = SvIV(*socket);

        // set the namespace
        cursor->ns = ns;

        // create the query
        full_query = newHV();
        cursor->query = newRV_noinc((SV*)full_query);

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
        cursor->limit = limit;
        cursor->skip = skip;

	// zero results fields
	cursor->num = 0;
	cursor->at = 0;

        // zero other fields
        cursor->fields = 0;
        cursor->opts = 0;
        cursor->started_iterating = 0;

        // STOP cursor setup

    OUTPUT:
        RETVAL


SV *
_find_one (self, ns, query)
	SV *self
        const char *ns
        SV *query
    PREINIT:
        SV *attr;
    INIT:
        attr = perl_mongo_call_reader (ST (0), "_oid_class");
        //q = new mongo::Query(perl_mongo_sv_to_bson (query, SvPV_nolen (attr)));
    CODE:
        //        ret = THIS->findOne(ns, *q);
        //        RETVAL = perl_mongo_bson_to_sv (SvPV_nolen (attr), ret);
    OUTPUT:
        RETVAL
    CLEANUP:
        SvREFCNT_dec (attr);

void
_insert (self, ns, object)
        SV *self
        const char *ns
        SV *object
    PREINIT:
        SV *oid_class, **socket;
        HV *this_hash;
        mongo_msg_header header;
        buffer buf;
    INIT:
        oid_class = perl_mongo_call_reader (ST (0), "_oid_class");
    CODE:
        this_hash = SvSTASH(SvRV(self));
        socket = hv_fetch(this_hash, "socket", strlen("socket"), 0);

        CREATE_BUF(INITIAL_BUF_SIZE);
        CREATE_HEADER(buf, ns, OP_INSERT);
        perl_mongo_sv_to_bson(&buf, object, SvPV_nolen (oid_class));
        serialize_size(buf.start, &buf);

        // sends
        mongo_link_say(SvIV(*socket), &buf);
        Safefree(buf.start);
      CLEANUP:
        SvREFCNT_dec (oid_class);

void
_remove (self, ns, query, just_one)
        SV *self
        const char *ns
        SV *query
        bool just_one
    PREINIT:
        SV *oid_class, **socket;
        HV *this_hash;
        mongo_msg_header header;
        buffer buf;
    INIT:
        oid_class = perl_mongo_call_reader (ST (0), "_oid_class");
    CODE:
        this_hash = SvSTASH(SvRV(self));
        socket = hv_fetch(this_hash, "socket", strlen("socket"), 0);

        CREATE_BUF(INITIAL_BUF_SIZE);
        CREATE_HEADER(buf, ns, OP_DELETE);
        serialize_int(&buf, (int)(just_one == 1));
        perl_mongo_sv_to_bson(&buf, query, SvPV_nolen (oid_class));
        serialize_size(buf.start, &buf);

        // sends
        mongo_link_say(SvIV(*socket), &buf);
        Safefree(buf.start);
    CLEANUP:
        SvREFCNT_dec (oid_class);

void
_update (self, ns, query, object, upsert)
        SV *self
        const char *ns
        SV *query
        SV *object
        bool upsert
    PREINIT:
        SV *oid_class, **socket;
        HV *this_hash;
        mongo_msg_header header;
        buffer buf;
    INIT:
        oid_class = perl_mongo_call_reader (ST (0), "_oid_class");
    CODE:
        this_hash = SvSTASH(SvRV(self));
        socket = hv_fetch(this_hash, "socket", strlen("socket"), 0);

        CREATE_BUF(INITIAL_BUF_SIZE);
        CREATE_HEADER(buf, ns, OP_UPDATE);
        serialize_int(&buf, upsert);
        perl_mongo_sv_to_bson(&buf, query, SvPV_nolen (oid_class));
        perl_mongo_sv_to_bson(&buf, object, SvPV_nolen (oid_class));
        serialize_size(buf.start, &buf);

        // sends
        mongo_link_say(SvIV(*socket), &buf);
        Safefree(buf.start);
    CLEANUP:
        SvREFCNT_dec (oid_class);

void
_ensure_index (self, ns, keys, unique=0)
	SV *self
        const char *ns
        SV *keys
        int unique
    PREINIT:
        SV *oid_class;
    INIT:
        oid_class = perl_mongo_call_reader (ST (0), "_oid_class");
        //obj = perl_mongo_sv_to_bson (keys, SvPV_nolen (oid_class));
    CODE:
        //THIS->ensureIndex(ns, obj, unique);
    CLEANUP:
        SvREFCNT_dec (oid_class);

NO_OUTPUT bool
_authenticate (self, dbname, username, password, is_digest=0)
	SV *self
        const char *dbname
        const char *username
        const char *password
        bool is_digest
    PREINIT:
        //std::string error_message;
        //std::string digest_password;
    INIT:
        /*if (is_digest) {
            digest_password = password;
        } else {
            digest_password = THIS->createPasswordDigest(username, password);
        }*/
    CODE:
        //RETVAL = THIS->auth(dbname, username, password, error_message, true);


void
connection_DESTROY (self)
          SV *self
     PREINIT:
         mongo_link *link;
     CODE:
         link = (mongo_link*)perl_mongo_get_ptr_from_instance(self);
         Safefree(link);
         printf("in destroy\n");
