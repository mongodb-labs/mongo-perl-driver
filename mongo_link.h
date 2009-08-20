
#ifndef MONGO_LINK_H
#define MONGO_LINK_H

#include "perl_mongo.h"

#ifdef WIN32
#include <winsock2.h>
#else
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <netdb.h>
#endif
#include <errno.h>

int mongo_connect(char *server, int port);
int mongo_get_sockaddr(struct sockaddr_in *addr, char *host, int port);

#endif
