#include "perl_mongo.h"


MODULE = MongoDB::OID  PACKAGE = MongoDB::OID

PROTOTYPES: DISABLE

SV *
_build_value (self, c_str)
        SV *self
        const char *c_str;
    PREINIT: 
        STRLEN len;
        char id[25];
    CODE:
        if (c_str && strlen(c_str) == 24) {
          memcpy(id, c_str, 24);
        }
        else {
          int i;
          char *movable, *id_str, *T;
          char data[12];
          unsigned t;

          int r1 = rand();
          int r2 = rand();
          
          char *inc = (char*)(void*)&r2;
          t = (unsigned) time(0);

          T = (char*)&t;
          data[0] = T[3];
          data[1] = T[2];
          data[2] = T[1];
          data[3] = T[0];

          memcpy(data+4, &r1, 4);
          data[8] = inc[3];
          data[9] = inc[2];
          data[10] = inc[1];
          data[11] = inc[0];

          perl_mongo_oid_create(data, id);
        }
        RETVAL = newSVpv (id, len);
    OUTPUT:
        RETVAL

    
        
