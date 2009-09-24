/*
 *  Copyright 2009 10gen, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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


// db ops
#define OP_REPLY 1
#define OP_MSG 1000
#define OP_UPDATE 2001
#define OP_INSERT 2002
#define OP_GET_BY_OID 2003
#define OP_QUERY 2004
#define OP_GET_MORE 2005 
#define OP_DELETE 2006
#define OP_KILL_CURSORS 2007 

// cursor flags
#define CURSOR_NOT_FOUND 1
#define CURSOR_ERR 2

#define MSG_HEADER_SIZE 16
#define REPLY_HEADER_SIZE (MSG_HEADER_SIZE+20)
#define INITIAL_BUF_SIZE 4096
// should only be 4Mb
#define MAX_RESPONSE_LEN 5242880
#define DEFAULT_CHUNK_SIZE (256*1024)

// if _id field should be added
#define PREP 1
#define NO_PREP 0

#define CREATE_MSG_HEADER(rid, rto, opcode)                     \
  header.length = 0;                                            \
  header.request_id = rid;                                      \
  header.response_to = rto;                                     \
  header.op = opcode;

#define CREATE_RESPONSE_HEADER(buf, ns, rto, opcode)                    \
  CREATE_MSG_HEADER(request_id++, rto, opcode);                         \
  APPEND_HEADER_NS(buf, ns, 0);

#define CREATE_HEADER_WITH_OPTS(buf, ns, opcode, opts)  \
  CREATE_MSG_HEADER(request_id++, 0, opcode);           \
  APPEND_HEADER_NS(buf, ns, opts);

#define CREATE_HEADER(buf, ns, opcode)          \
  CREATE_RESPONSE_HEADER(buf, ns, 0, opcode);                    

#define APPEND_HEADER(buf, opts) buf.pos += INT_32;       \
  perl_mongo_serialize_int(&buf, header.request_id);                 \
  perl_mongo_serialize_int(&buf, header.response_to);                \
  perl_mongo_serialize_int(&buf, header.op);                         \
  perl_mongo_serialize_int(&buf, opts);                                

#define APPEND_HEADER_NS(buf, ns, opts)                 \
  APPEND_HEADER(buf, opts);                             \
  perl_mongo_serialize_string(&buf, ns, strlen(ns));              

#define CREATE_BUF(size)                                \
  Newx(buf.start, size, char);				\
  buf.pos = buf.start;                                  \
  buf.end = buf.start + size;


typedef struct {
  int length;
  int request_id;
  int response_to;
  int op;
} mongo_msg_header;

typedef struct {
  int paired;
  // master socket
  int master;
  int ts;
  int auto_reconnect;
  union {
    struct {
      char *host;
      int port;
      int socket;
      int connected;
    } single;
    struct {
      char *left_host;
      int left_port;
      int left_socket;
      int left_connected;

      char *right_host;
      int right_port;
      int right_socket;
      int right_connected;
    } pair;
  } server;
} mongo_link;

typedef struct {
  // collection namespace
  const char *ns;

  // fields to send
  SV *query;
  SV *fields;
  int limit;
  int skip;
  int opts;

  // response header
  mongo_msg_header header;
  // response fields
  int flag;
  long long cursor_id;
  int start;
  // number of results used
  int at;
  // number results returned
  int num;
  // results
  buffer buf;

  int started_iterating;

} mongo_cursor;

int mongo_link_connect(mongo_link *link);
int mongo_link_say(SV *self, mongo_link *link, buffer *buf);
int mongo_link_hear(SV *self, mongo_link *link, mongo_cursor *cursor);
int perl_mongo_link_master(SV *self, mongo_link *link);

#endif
