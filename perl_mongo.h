#ifndef PERL_MONGO
#define PERL_MONGO

#undef VERSION
#include <client/dbclient.h>

// dbclient.h redefines assert, so we'll redefine it back again.
#define assert(what) PERL_DEB(                                       \
        ((what) ? ((void) 0) :                                       \
         (Perl_croak(aTHX_ "Assertion %s failed: file \"" __FILE__   \
                     "\", line %d", STRINGIFY(what), __LINE__),      \
          PerlProc_exit(1),                                          \
          (void) 0)))

extern "C" {

#define PERL_GCC_BRACE_GROUPS_FORBIDDEN

#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#define PERL_MONGO_CALL_BOOT(name)  perl_mongo_call_xs (aTHX_ name, cv, mark)

void perl_mongo_call_xs (pTHX_ void (*subaddr) (pTHX_ CV *cv), CV *cv, SV **mark);
SV *perl_mongo_call_reader (SV *self, const char *reader);
void perl_mongo_attach_ptr_to_instance (SV *self, void *ptr);
void *perl_mongo_get_ptr_from_instance (SV *self);
SV *perl_mongo_construct_instance (const char *klass, ...);
SV *perl_mongo_construct_instance_va (const char *klass, va_list ap);
SV *perl_mongo_construct_instance_with_magic (const char *klass, void *ptr, ...);
SV *perl_mongo_bson_to_sv (const char *oid_class, mongo::BSONObj obj);
mongo::BSONObj perl_mongo_sv_to_bson (SV *sv, const char *oid_class);

}

#endif
