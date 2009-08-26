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

static int mongo_link_sockaddr(struct sockaddr_in *addr, char *host, int port);
static int mongo_link_reader(int socket, void *dest, int len);
static int do_connect(char *host, int port);
static int check_connection(mongo_link *link);

int mongo_link_connect(mongo_link *link) {
  if (link->paired) {
    link->server.pair.left_socket = do_connect(link->server.pair.left_host, link->server.pair.left_port);
    link->server.pair.left_connected = (link->server.pair.left_socket != 0);

    link->server.pair.right_socket = do_connect(link->server.pair.right_host, link->server.pair.right_port);
    link->server.pair.right_connected = (link->server.pair.right_socket != 0);

    return link->server.pair.left_connected && link->server.pair.right_connected;
  }

  link->server.single.socket = do_connect(link->server.single.host, link->server.single.port);
  link->server.single.connected = link->server.single.socket;
  return link->server.single.connected;
}

static int do_connect(char *host, int port) {
  int sock;
  struct sockaddr_in addr, check_connect;
  fd_set rset, wset;

  struct timeval timeout;

  // start unconnected
  int connected = 0;

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
  if (!(sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP))) {
    return 0;
  }
#endif

  // timeout
  timeout.tv_sec = 20;
  timeout.tv_usec = 0;

  // get addresses
  if (!mongo_link_sockaddr(&addr, host, port)) {
    return 0;
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
  if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
#ifdef WIN32
    errno = WSAGetLastError();
    if (errno != WSAEINPROGRESS &&
		errno != WSAEWOULDBLOCK)
#else
    if (errno != EINPROGRESS)
#endif
    {
      return 0;
    }

    if (!select(sock+1, &rset, &wset, 0, &timeout)) {
      return 0;
    }

    size = sizeof(check_connect);

    connected = getpeername(sock, (struct sockaddr*)&addr, &size);
    if (connected == -1) {
      return 0;
    }
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
int mongo_link_say(SV *self, mongo_link *link, buffer *buf) {
  int sock, sent;

  sock = perl_mongo_link_master(self, link);
  sent = send(sock, (const char*)buf->start, buf->pos-buf->start, 0);

  if (sent == -1) {
    if (check_connection(link)) {
      sock = perl_mongo_link_master(self, link);
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
int mongo_link_hear(SV *self, mongo_link *link, mongo_cursor *cursor) {
  int sock = perl_mongo_link_master(self, link);
  int num_returned = 0;

  // if this fails, we might be disconnected... but we're probably
  // just out of results
  if (recv(sock, (char*)&cursor->header.length, INT_32, 0) == -1) {
    return 0;
  }

  // make sure we're not getting crazy data
  if (cursor->header.length > MAX_RESPONSE_LEN ||
      cursor->header.length < REPLY_HEADER_SIZE) {
    croak("bad response length: %d, max: %d, did the db assert?\n", cursor->header.length, MAX_RESPONSE_LEN);
    return 0;
  }

  if (recv(sock, (char*)&cursor->header.request_id, INT_32, 0) == -1 ||
      recv(sock, (char*)&cursor->header.response_to, INT_32, 0) == -1 ||
      recv(sock, (char*)&cursor->header.op, INT_32, 0) == -1 ||
      recv(sock, (char*)&cursor->flag, INT_32, 0) == -1 ||
      recv(sock, (char*)&cursor->cursor_id, INT_64, 0) == -1 ||
      recv(sock, (char*)&cursor->start, INT_32, 0) == -1 ||
      recv(sock, (char*)&num_returned, INT_32, 0) == -1) {
    return 0;
  }

  // create buf
  cursor->header.length -= INT_32*9;

  // point buf.start at buf's first char
  if (!cursor->buf.start) {
    Newx(cursor->buf.start, cursor->header.length, char);
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
    return 0;
  }
  
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
  int now = time(0);

  if (!link->auto_reconnect ||
      (link->paired && link->server.pair.left_connected && link->server.pair.right_connected) ||
      (!link->paired && link->server.single.connected) ||
      now-link->ts < 2) {
    return 1;
  }

  link->ts = now;

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

  if (link->paired) {
    link->server.pair.left_connected = 0;
    link->server.pair.right_connected = 0;
  }
  else {
    link->server.single.connected = 0;
  }

  return mongo_link_connect(link);
}

int perl_mongo_link_master(SV *self, mongo_link *link) {
  SV *master;
  int side;

  if (!link->paired) {
    return link->server.single.socket;
  }

  if (link->server.pair.left_socket == link->master &&
      link->server.pair.left_connected) {
    return link->master;
  }
  else if (link->server.pair.right_socket == link->master &&
           link->server.pair.right_connected) {
    return link->master;
  }

  master = perl_mongo_call_method(self, "find_master", 0);
  side = SvIV(master);

  if (side == 0) {
    return link->master = link->server.pair.left_socket;
  }
  else if (side == 1) {
    return link->master = link->server.pair.right_socket;
  }
  croak("error finding master");
}
