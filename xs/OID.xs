#include "perl_mongo.h"

MODULE = MongoDB::OID  PACKAGE = MongoDB::OID

PROTOTYPES: DISABLE

SV *
_build_value (self, c_str)
        SV *self
        const char *c_str;
    PREINIT: 
        STRLEN len;
        char *data;
    INIT:
        Newx(data, 12, char);
    CODE:
        if (c_str && strlen(c_str) == 24) {
          data = (char*)c_str;
        }
        else {
          unsigned t;
          char *T;

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
        }
        RETVAL = newSVpv (data, len);
    OUTPUT:
        RETVAL

    
        
