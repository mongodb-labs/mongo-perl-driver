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
static int mongo_link_reader(connection* conn, void *dest, int len);

/**
 * Waits "timeout" ms for the socket to be ready.  Returns 1 on success, 0 on
 * failure.
 */
static int mongo_link_timeout(int socket, time_t timeout);

static void set_timeout(int socket, time_t timeout) {
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

/*
 * Returns -1 on failure, the socket fh on success.
 *
 * Note: this cannot return 0 on failure, because reconnecting sometimes makes
 * the fh 0 (briefly).
 */
 //TODO: make function pointer to wrap send/recieve calls
 //TODO: change functions return conncetions vs sockets 
connection* perl_mongo_connect(char *host, int port, int timeout, bool ssl) {
    connection* _conn;
    _conn = malloc(sizeof (connection));
    if(ssl)
        _conn = ssl_connect(host, port, timeout);
    else
        _conn->socket = non_ssl_connect(host, port, timeout);
    return _conn;
}

int non_ssl_connect(char *host, int port, int timeout){
    int sock, status, connected = 0;
      struct sockaddr_in addr;

    #ifdef WIN32
      WORD version;
      WSADATA wsaData;
      int error;
      u_long no = 0;
      const char yes = 1;

      version = MAKEWORD(2,2);
      error = WSAStartup(version, &wsaData);

      if (error != 0) {
        return -1;
      }

      // create socket
      sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
      if (sock == INVALID_SOCKET) {
        return -1;
      }

    #else
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
      set_timeout(sock, timeout);

    #ifdef WIN32
      ioctlsocket(sock, FIONBIO, (u_long*)&yes);
    #else
      fcntl(sock, F_SETFL, O_NONBLOCK);
    #endif

      // connect
      status = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
      if (status == -1) {
        socklen_t size;

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

        if (!mongo_link_timeout(sock, timeout)) {
          return -1;
        }

        size = sizeof(addr);

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

//TODO: timeout...not sure if its different for ssl
connection* ssl_connect(char *host, int port, int timeout){
    return sslConnect(host, port, timeout);
}

static int mongo_link_timeout(int sock, time_t to) {
  struct timeval timeout, now, prev;

  if (to <= 0) {
    return 1;
  }

  timeout.tv_sec = to > 0 ? (to / 1000) : 20;
  timeout.tv_usec = to > 0 ? ((to % 1000) * 1000) : 0;

  // initialize prev, in case we get interrupted
  if (gettimeofday(&prev, 0) == -1) {
    return 0;
  }

  while (1) {
    fd_set rset, wset, eset;
    int sock_status;

    FD_ZERO(&rset);
    FD_SET(sock, &rset);
    FD_ZERO(&wset);
    FD_SET(sock, &wset);
    FD_ZERO(&eset);
    FD_SET(sock, &eset);

    sock_status = select(sock+1, &rset, &wset, &eset, &timeout);

    // error
    if (sock_status == -1) {

#ifdef WIN32
      errno = WSAGetLastError();
#endif

      if (errno == EINTR) {
        if (gettimeofday(&now, 0) == -1) {
          return 0;
        }

        // update timeout
        timeout.tv_sec -= (now.tv_sec - prev.tv_sec);
        timeout.tv_usec -= (now.tv_usec - prev.tv_usec);

        // update prev
        prev.tv_sec = now.tv_sec;
        prev.tv_usec = now.tv_usec;
      }

      // check if we have an invalid timeout before continuing
      if (timeout.tv_sec >= 0 || timeout.tv_usec >= 0) {
        continue;
      }

      // if this isn't a EINTR, it's a fatal error
      return 0;
    }

    // timeout
    if (sock_status == 0 && !FD_ISSET(sock, &wset) && !FD_ISSET(sock, &rset)) {
      return 0;
    }

    if (FD_ISSET(sock, &eset)) {
      return 0;
    }

    if (FD_ISSET(sock, &wset) || FD_ISSET(sock, &rset)) {
      break;
    }
  }

  return 1;
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
  connection* conn;

  if ((conn = perl_mongo_master(link_sv, 1)) == NULL) {
    return -1;
  }
  
  sent = send(conn->socket, (const char*)buf->start, buf->pos-buf->start, 0);
  
  if (sent == -1) {
    set_disconnected(link_sv);
  }
  
  return sent;
}


static int get_header(connection* conn, SV *cursor_sv, SV *link_sv) {
  mongo_cursor *cursor;

  cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(cursor_sv, &cursor_vtbl);

  if (recv(conn->socket, (char*)&cursor->header.length, INT_32, 0) != INT_32) {
    set_disconnected(link_sv);
    return 0;
  }

  cursor->header.length = MONGO_32(cursor->header.length);

  // make sure we're not getting crazy data
  if (cursor->header.length > MAX_RESPONSE_LEN ||
      cursor->header.length < REPLY_HEADER_SIZE) {

    set_disconnected(link_sv);
    return 0;
  }

  if (recv(conn->socket, (char*)&cursor->header.request_id, INT_32, 0) != INT_32 ||
      recv(conn->socket, (char*)&cursor->header.response_to, INT_32, 0) != INT_32 ||
      recv(conn->socket, (char*)&cursor->header.op, INT_32, 0) != INT_32) {
    return 0;
  }

  cursor->header.request_id = MONGO_32(cursor->header.request_id);
  cursor->header.response_to = MONGO_32(cursor->header.response_to);
  cursor->header.op = MONGO_32(cursor->header.op);

  return 1;
}

/*
 * Gets a reply from the MongoDB server and
 * creates a cursor for it
 */
int mongo_link_hear(SV *cursor_sv) {
  connection* conn;
  int num_returned = 0, timeout = -1;
  mongo_cursor *cursor;
  mongo_link *link;
  SV *link_sv, *request_id_sv, *timeout_sv;

  cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(cursor_sv, &cursor_vtbl);
  link_sv = perl_mongo_call_reader(cursor_sv, "_connection");
  link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_sv, &connection_vtbl);
  timeout_sv = perl_mongo_call_reader(link_sv, "query_timeout");

  if ((conn->socket = perl_mongo_master(link_sv, 0)) == NULL) {
    set_disconnected(link_sv);
    SvREFCNT_dec(link_sv);
    croak("can't get db response, not connected");
  }

  timeout = SvIV(timeout_sv);
  SvREFCNT_dec(timeout_sv);

  // set a timeout
  if (timeout >= 0) {
    struct timeval t;
    fd_set readfds;

    t.tv_sec = timeout / 1000 ;
    t.tv_usec = (timeout % 1000) * 1000;

    FD_ZERO(&readfds);
    FD_SET(conn->socket, &readfds);

    select(conn->socket+1, &readfds, NULL, NULL, &t);

    if (!FD_ISSET(conn->socket, &readfds)) {
      SvREFCNT_dec(link_sv);
      croak("recv timed out (%d ms)", timeout);
      return 0;
    }
  }

  if (get_header(conn->socket, cursor_sv, link_sv) == 0) {
    SvREFCNT_dec(link_sv);
    croak("can't get db response, not connected");
    return 0;
  }

  request_id_sv = perl_mongo_call_reader(cursor_sv, "_request_id");
  while (SvIV(request_id_sv) != cursor->header.response_to) {
    char temp[4096];
    int len = cursor->header.length - 36;

    if (SvIV(request_id_sv) < cursor->header.response_to) {
      SvREFCNT_dec(link_sv);
      SvREFCNT_dec(request_id_sv);
      croak("missed the response we wanted, please try again");
      return 0;
    }

    if (recv(conn->socket, (char*)temp, 20, 0) == -1) {
      SvREFCNT_dec(link_sv);
      SvREFCNT_dec(request_id_sv);
      croak("couldn't get header response to throw out");
      return 0;
    }

    do {
      int temp_len = len > 4096 ? 4096 : len;
      len -= temp_len;

      if (mongo_link_reader(conn->socket, (void*)temp, temp_len) == -1) {
        SvREFCNT_dec(link_sv);
        SvREFCNT_dec(request_id_sv);
        croak("couldn't get response to throw out");
        return 0;
      }
    } while (len > 0);

    if (get_header(conn->socket, cursor_sv, link_sv) == 0) {
      SvREFCNT_dec(link_sv);
      SvREFCNT_dec(request_id_sv);
      croak("invalid header received");
      return 0;
    }
  }
  SvREFCNT_dec(request_id_sv);
  
  if (recv(conn->socket, (char*)&cursor->flag, INT_32, 0) == -1 ||
      recv(conn->socket, (char*)&cursor->cursor_id, INT_64, 0) == -1 ||
      recv(conn->socket, (char*)&cursor->start, INT_32, 0) == -1 ||
      recv(conn->socket, (char*)&num_returned, INT_32, 0) == -1) {
    SvREFCNT_dec(link_sv);
    croak("%s", strerror(errno));
    return 0;
  }
  SvREFCNT_dec(link_sv);

  cursor->flag = MONGO_32(cursor->flag);
  // if zero-th bit is set, cursor is invalid
  if (cursor->flag & 1) {
      cursor->num = 0;
      croak("cursor not found");
  }

  cursor->cursor_id = MONGO_64(cursor->cursor_id);
  cursor->start = MONGO_32(cursor->start);
  num_returned = MONGO_32(num_returned);

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

  if (mongo_link_reader(conn->socket, cursor->buf.pos, cursor->header.length) == -1) {
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
static int mongo_link_reader(connection* conn, void *dest, int len) {
  int num = 1, read = 0;

  // this can return FAILED if there is just no more data from db
  while(read < len && num > 0) {
    int temp_len = (len - read) > 4096 ? 4096 : (len - read);

    // windows gives a WSAEFAULT if you try to get more bytes
    num = recv(conn->socket, (char*)dest, temp_len, 0);

    if (num < 0) {
      return -1;
    }

    dest = (char*)dest + num;
    read += num;
  }
  return read;
}


/*
 * closes sockets and sets "connected" to 0
 */
void set_disconnected(SV *link_sv) {
  mongo_link *link;

  link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_sv, &connection_vtbl);

  // check if there's nothing to do
  if (link->master == 0 || link->master->conn->connected == 0) {
      return;
  }

#ifdef WIN32
  shutdown(link->master->conn->socket, 2);
  closesocket(link->master->conn->socket);
  WSACleanup();
#else
  close(link->master->conn->socket);
#endif

  link->master->conn->connected = 0;

  // TODO: set $self->_master to 0?
  if (link->copy) {
      link->master = 0;
      perl_mongo_call_method(link_sv, "_master", G_DISCARD, 1, &PL_sv_no);
  }
}

connection* perl_mongo_master(SV *link_sv, int auto_reconnect) {
  SV *master;
  mongo_link *link;

  link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_sv, &connection_vtbl);

  if (link->master && link->master->conn->connected) {
      return link->master->conn;
  }
  // if we didn't have a connection above and this isn't a connection holder
  if (!link->copy) {
      // if this is a real connection, try to reconnect
      if (auto_reconnect && link->auto_reconnect) {
          perl_mongo_call_method(link_sv, "connect", G_DISCARD, 0);
          if (link->master && link->master->conn->connected) {
              return link->master->conn;
          }
      }

      return -1;
  }

  master = perl_mongo_call_method(link_sv, "get_master", 0, 0);
  if (SvROK(master)) {
    mongo_link *m_link;

    m_link = (mongo_link*)perl_mongo_get_ptr_from_instance(master, &connection_vtbl);
    link->copy = 1;
    link->master = m_link->master;

    return link->master->conn;
  }

  link->master = 0;
  return NULL;
}


//ssl
// Establish a regular tcp connection
int tcpConnect(char *hostname, int port, int timout)
{
    int error, handle;
    struct hostent *host;
    struct sockaddr_in server;
    
    host = gethostbyname (hostname);
    handle = socket (AF_INET, SOCK_STREAM, 0);
    if (handle == -1){
        perror ("Socket");
        handle = 0;
    }
    else{
        server.sin_family = AF_INET;
        server.sin_port = htons (port);
        server.sin_addr = *((struct in_addr *) host->h_addr);
        bzero (&(server.sin_zero), 8);
    
        error = connect (handle, (struct sockaddr *) &server, sizeof (struct sockaddr));
        if (error == -1){
            perror ("Connect");
            handle = 0;
        }
    }
    
    return handle;
}

// Establish a connection using an SSL layer
connection *sslConnect(char *host, int port, int timeout)
{
    connection *c;
    
    c = malloc (sizeof (connection));
    c->sslHandle = NULL;
    c->sslContext = NULL;
    
    c->socket = tcpConnect(host, port, timeout);
    if (c->socket){
        //Register the error strings for libcrypto & libssl
        SSL_load_error_strings();
        
        //Register the available ciphers and digests
        SSL_library_init();
    
        //New context saying we are a client, and using SSL 2 or 3
        c->sslContext = SSL_CTX_new(SSLv23_client_method ());
        if (c->sslContext == NULL)
            ERR_print_errors_fp(stderr);
    
        //Create an SSL struct for the connection
        c->sslHandle = SSL_new(c->sslContext);
        if(c->sslHandle == NULL)
            ERR_print_errors_fp(stderr);
    
        //Connect the SSL struct to our connection
        if(!SSL_set_fd(c->sslHandle, c->socket))
            ERR_print_errors_fp(stderr);
    
        //Initiate SSL handshake
        if(SSL_connect(c->sslHandle) != 1)
            ERR_print_errors_fp(stderr);
    }
    else
        perror("Connect failed");
    
    return c;
}

// Disconnect & free connection struct
void sslDisconnect(connection *c)
{
    if(c->socket)
      close(c->socket);
      
    if(c->sslHandle){
        SSL_shutdown(c->sslHandle);
        SSL_free(c->sslHandle);
    }
    
    if(c->sslContext)
      SSL_CTX_free(c->sslContext);
    
    free(c);
}

// Read all available text from the connection
char *sslRead (connection *c)
{
  const int readSize = 1024;
  char *rc = NULL;
  int received, count = 0;
  char buffer[1024];

  if (c)
    {
      while (1)
        {
          if (!rc)
            rc = malloc (readSize * sizeof (char) + 1);
          else
            rc = realloc (rc, (count + 1) *
                          readSize * sizeof (char) + 1);

          received = SSL_read (c->sslHandle, buffer, readSize);
          buffer[received] = '\0';

          if (received > 0)
            strcat (rc, buffer);

          if (received < readSize)
            break;
          count++;
        }
    }

  return rc;
}

// Write text to the connection
void sslWrite (connection *c, char *text)
{
  if (c)
    SSL_write (c->sslHandle, text, strlen (text));
}

// Very basic main: we send GET / and print the response.
//int main (int argc, char **argv)
//{
//  connection *c;
//  char *response;
//
//  c = sslConnect ();
//
//  sslWrite (c, "GET /\r\n\r\n");
//  response = sslRead (c);
//
//  printf ("%s\n", response);
//
//  sslDisconnect (c);
//  free (response);
//
//  return 0;
//}
//
