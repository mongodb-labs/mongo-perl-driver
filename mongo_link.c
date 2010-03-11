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

#include "mongo_link.h"
#include "perl_mongo.h"

static int mongo_link_sockaddr(struct sockaddr_in *addr, char *host, int port);
static int mongo_link_reader(int socket, void *dest, int len);
static int do_connect(char *host, int port, int timeout);
static int check_connection(mongo_link *link);
inline void set_disconnected(mongo_link *link);

int mongo_link_connect(mongo_link *link) {
  if (link->paired) {
    link->server.pair.left_socket = do_connect(link->server.pair.left_host, link->server.pair.left_port, link->timeout);
    link->server.pair.left_connected = (link->server.pair.left_socket != -1);

    link->server.pair.right_socket = do_connect(link->server.pair.right_host, link->server.pair.right_port, link->timeout);
    link->server.pair.right_connected = (link->server.pair.right_socket != -1);

    return link->server.pair.left_connected || link->server.pair.right_connected;
  }

  link->server.single.socket = do_connect(link->server.single.host, link->server.single.port, link->timeout);
  link->server.single.connected = (link->server.single.socket != -1);
  return link->server.single.connected;
}

static int do_connect(char *host, int port, int timeout) {
  int sock, status, connected = 0;
  struct sockaddr_in addr, check_connect;
  fd_set rset, wset;

  // timeout
  struct timeval timeout_struct;

#ifdef WIN32
  WORD version;
  WSADATA wsaData;
  int size, error;
  u_long no = 0;
  const char yes = 1;

  version = MAKEWORD(2,2);
  error = WSAStartup(version, &wsaData);

  if (error != 0) {
    return 0;
  }

  // create socket
  sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sock == INVALID_SOCKET) {
    return 0;
  }

#else
  uint size;
  int yes = 1;

  // create socket
  if ((sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1) {
    croak("couldn't create socket: %s\n", strerror(errno));
    return -1;
  }
#endif

  // get addresses
  if (!mongo_link_sockaddr(&addr, host, port)) {
    return -1;
  }

  setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &yes, INT_32);
  setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &yes, INT_32);

#ifdef WIN32
  ioctlsocket(sock, FIONBIO, (u_long*)&yes);
#else
  fcntl(sock, F_SETFL, O_NONBLOCK);
#endif

  FD_ZERO(&rset);
  FD_SET(sock, &rset);
  FD_ZERO(&wset);
  FD_SET(sock, &wset);

  // connect
  status = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
  if (status == -1) {
#ifdef WIN32
    errno = WSAGetLastError();
    if (errno != WSAEINPROGRESS &&
		errno != WSAEWOULDBLOCK)
#else
    if (errno != EINPROGRESS)
#endif
    {
      return -1;
    }

    timeout_struct.tv_sec = timeout / 1000;
    timeout_struct.tv_usec = (timeout % 1000) * 1000;
    if (!select(sock+1, &rset, &wset, 0, &timeout_struct)) {
      return -1;
    }

    size = sizeof(check_connect);

    connected = getpeername(sock, (struct sockaddr*)&addr, &size);
    if (connected == -1) {
      return -1;
    }
  }
  else if (status == 0) {
    connected = 1;
  }
  
// reset flags
#ifdef WIN32
  ioctlsocket(sock, FIONBIO, &no);
#else
  fcntl(sock, F_SETFL, 0);
#endif
  return sock;
}


static int mongo_link_sockaddr(struct sockaddr_in *addr, char *host, int port) {
  struct hostent *hostinfo;

  addr->sin_family = AF_INET;
  addr->sin_port = htons(port);
  hostinfo = (struct hostent*)gethostbyname(host);

  if (!hostinfo) {
    return 0;
  }

#ifdef WIN32
  addr->sin_addr.s_addr = ((struct in_addr*)(hostinfo->h_addr))->s_addr;
#else
  addr->sin_addr = *((struct in_addr*)hostinfo->h_addr);
#endif

  return 1;
}


/*
 * Sends a message to the MongoDB server
 */
int mongo_link_say(SV *link_sv, buffer *buf) {
  int sock, sent;
  mongo_link *link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_sv);

  if (!check_connection(link)) {
    croak("can't get db response, not connected");
    return -1;
  }

  sock = perl_mongo_link_master(link_sv);
  sent = send(sock, (const char*)buf->start, buf->pos-buf->start, 0);

  if (sent == -1) {
    if (check_connection(link)) {
      sock = perl_mongo_link_master(link_sv);
      sent = send(sock, (const char*)buf->start, buf->pos-buf->start, 0);
    }
    else {
      return -1;
    }
  }

  return sent;
}


/*
 * Gets a reply from the MongoDB server and
 * creates a cursor for it
 */
int mongo_link_hear(SV *cursor_sv) {
  int sock;
  int num_returned = 0;
  mongo_cursor *cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(cursor_sv);
  SV *link_sv = perl_mongo_call_reader(cursor_sv, "_connection");
  mongo_link *link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_sv);

  if (!check_connection(link)) {
    SvREFCNT_dec(link_sv);
    croak("can't get db response, not connected");
    return 0;
  }
  
  sock = perl_mongo_link_master(link_sv);

  // if this fails, we might be disconnected... but we're probably
  // just out of results
  if (recv(sock, (char*)&cursor->header.length, INT_32, 0) == -1) {
    SvREFCNT_dec(link_sv);
    return 0;
  }

  cursor->header.length = MONGO_32(cursor->header.length);

  // make sure we're not getting crazy data
  if (cursor->header.length > MAX_RESPONSE_LEN ||
      cursor->header.length < REPLY_HEADER_SIZE) {

    set_disconnected(link);

    if (!check_connection(link)) {
      SvREFCNT_dec(link_sv);
      croak("bad response length: %d, max: %d, did the db assert?\n", cursor->header.length, MAX_RESPONSE_LEN);
      return 0;
    }
  }
  
  if (recv(sock, (char*)&cursor->header.request_id, INT_32, 0) == -1 ||
      recv(sock, (char*)&cursor->header.response_to, INT_32, 0) == -1 ||
      recv(sock, (char*)&cursor->header.op, INT_32, 0) == -1 ||
      recv(sock, (char*)&cursor->flag, INT_32, 0) == -1 ||
      recv(sock, (char*)&cursor->cursor_id, INT_64, 0) == -1 ||
      recv(sock, (char*)&cursor->start, INT_32, 0) == -1 ||
      recv(sock, (char*)&num_returned, INT_32, 0) == -1) {
    SvREFCNT_dec(link_sv);
    return 0;
  }

  cursor->header.request_id = MONGO_32(cursor->header.request_id);
  cursor->header.response_to = MONGO_32(cursor->header.response_to);
  cursor->header.op = MONGO_32(cursor->header.op);
  cursor->flag = MONGO_32(cursor->flag);
  cursor->cursor_id = MONGO_64(cursor->cursor_id);
  cursor->start = MONGO_32(cursor->start);
  num_returned = MONGO_32(num_returned);

  // create buf
  cursor->header.length -= INT_32*9;

  // point buf.start at buf's first char
  if (!cursor->buf.start) {
    New(0, cursor->buf.start, cursor->header.length, char);
    cursor->buf.end = cursor->buf.start + cursor->header.length;
  }
  else if (cursor->buf.end - cursor->buf.start < cursor->header.length) { 
    Renew(cursor->buf.start, cursor->header.length, char);
    cursor->buf.end = cursor->buf.start + cursor->header.length;
  }
  cursor->buf.pos = cursor->buf.start;

  if (mongo_link_reader(sock, cursor->buf.pos, cursor->header.length) == -1) {
#ifdef WIN32
    croak("WSA error getting database response: %d\n", WSAGetLastError());
#else
    croak("error getting database response: %s\n", strerror(errno));
#endif 
    SvREFCNT_dec(link_sv);
    return 0;
  }
  
  SvREFCNT_dec(link_sv);
  cursor->num += num_returned;
  return num_returned > 0;
}
 

/*
 * Low-level func to get a response from the MongoDB server
 */
static int mongo_link_reader(int socket, void *dest, int len) {
  int num = 1, r = 0;

  // this can return FAILED if there is just no more data from db
  while(r < len && num > 0) {

#ifdef WIN32
    // windows gives a WSAEFAULT if you try to get more bytes
    num = recv(socket, (char*)dest, 4096, 0);
#else
    num = recv(socket, (char*)dest, len, 0);
#endif

    if (num < 0) {
      return -1;
    }

    dest = (char*)dest + num;
    r += num;
  }
  return r;
}

static int check_connection(mongo_link *link) {
  if (!link->auto_reconnect ||
      (link->paired && (link->server.pair.left_connected || link->server.pair.right_connected)) ||
      (!link->paired && link->server.single.connected)) {
    return 1;
  }

  link->ts = time(0);

#ifdef WIN32
  if (link->paired) {
    closesocket(link->server.pair.left_socket);
    closesocket(link->server.pair.right_socket);
  }
  else {
    closesocket(link->server.single.socket);
  }
  WSACleanup();
#else
  if (link->paired) {
    close(link->server.pair.left_socket);
    close(link->server.pair.right_socket);
  }
  else {
    close(link->server.single.socket);
  }
#endif

  set_disconnected(link);

  return mongo_link_connect(link);
}

inline void set_disconnected(mongo_link *link) {
  if (link->paired) {
    link->server.pair.left_connected = 0;
    link->server.pair.right_connected = 0;
    link->master = -1;
  }
  else {
    link->server.single.connected = 0;
  }
}

int perl_mongo_link_master(SV *link_sv) {
  SV *master;
  int side;
  mongo_link *link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_sv);

  if (!link->paired) {
    return link->server.single.socket;
  }

  if (link->master != -1) {
    if (link->server.pair.left_socket == link->master &&
        link->server.pair.left_connected) {
      return link->master;
    }
    else if (link->server.pair.right_socket == link->master &&
             link->server.pair.right_connected) {
      return link->master;
    }
  }

  master = perl_mongo_call_method(link_sv, "find_master", 0);
  side = SvIV(master);

  if (side == 0) {
    link->server.pair.left_connected = 1;
    return link->master = link->server.pair.left_socket;
  }
  else if (side == 1) {
    link->server.pair.right_connected = 1;
    return link->master = link->server.pair.right_socket;
  }

  croak("couldn't find master");
}
