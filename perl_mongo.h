#ifndef PERL_MONGO
#define PERL_MONGO

#include <mongo/client/dbclient.h>

extern "C" {

#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#define PERL_MONGO_CALL_BOOT(name)  perl_mongo_call_xs (aTHX_ name, cv, mark)

void perl_mongo_call_xs (pTHX_ void (*subaddr) (pTHX_ CV *cv), CV *cv, SV **mark);
SV *perl_mongo_call_reader (SV *self, const char *reader);
void perl_mongo_attach_ptr_to_instance (SV *self, void *ptr);
void *perl_mongo_get_ptr_from_instance (SV *self);
SV *perl_mongo_construct_instance_with_magic (const char *klass, void *ptr);
SV *perl_mongo_bson_to_sv (mongo::BSONObj obj);

}

#endif
