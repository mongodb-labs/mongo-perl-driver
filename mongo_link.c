/*
 *  Copyright 2009-2013 MongoDB, Inc.
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
static int mongo_link_reader(mongo_link* link, void *dest, int len);

/**
 * Waits "timeout" ms for the socket to be ready.  Returns 1 on success, 0 on
 * failure.
 */
static int mongo_link_timeout(int socket, time_t timeout);

static bool timeval_add(struct timeval *result, struct timeval *t2, struct timeval *t1) {
    long int sum = (t2->tv_usec + 1000000 * t2->tv_sec) + (t1->tv_usec + 1000000 * t1->tv_sec);
    result->tv_sec = sum / 1000000;
    result->tv_usec = sum % 1000000;
    return (sum<0);
}

static bool timeval_subtract(struct timeval *result, struct timeval *t2, struct timeval *t1) {
    long int delta = (t2->tv_usec + 1000000 * t2->tv_sec) - (t1->tv_usec + 1000000 * t1->tv_sec);
    result->tv_sec = delta / 1000000;
    result->tv_usec = delta % 1000000;
    return (delta<0);
}

#ifdef MONGO_SASL
static void sasl_authenticate( SV *client, mongo_link *link ) { 
  Gsasl *ctx = NULL;
  Gsasl_session *session;
  SV *username, *mechanism, *conv_id;
  HV *result;       /* response document from mongod */
  char *p, *buf;    /* I/O buffers for gsasl */
  int rc;
  char out_buf[8192];

  /* check that we are connected before attempting a SASL conversation;
     otherwise we will end up in an infinite loop */
  if ( ! link->master->connected ) { 
    croak( "MongoDB: Could not begin SASL authentication without connection." );
  }

  mechanism = perl_mongo_call_method( client, "sasl_mechanism", 0, 0 );
  if ( !SvOK( mechanism ) ) { 
    croak( "MongoDB: Could not retrieve SASL mechanism from client object\n" );
  }

  if ( strncmp( "PLAIN", SvPV_nolen( mechanism ), 5 ) == 0 ) { 
    /* SASL PLAIN does not require a libgsasl conversation loop, so we can handle it elsewhere */
    perl_mongo_call_method( client, "_sasl_plain_authenticate", 0, 0 );
    return;
  }

  if ( ( rc = gsasl_init( &ctx ) ) != GSASL_OK ) { 
    croak( "MongoDB: Cannot initialize libgsasl (%d): %s\n", rc, gsasl_strerror(rc) );  
  }

  if ( ( rc = gsasl_client_start( ctx, SvPV_nolen( mechanism ), &session ) ) != GSASL_OK ) { 
    croak( "MongoDB: Cannot initialize SASL client (%d): %s\n", rc, gsasl_strerror(rc) );
  }

  username = perl_mongo_call_method( client, "username", 0, 0 );
  if ( !SvOK( username ) ) { 
    croak( "MongoDB: Cannot start SASL session without username. Specify username in constructor\n" );
  }
 
  gsasl_property_set( session, GSASL_SERVICE,  "mongodb" );
  gsasl_property_set( session, GSASL_HOSTNAME, link->master->host );
  gsasl_property_set( session, GSASL_AUTHID,   SvPV_nolen( username ) ); 

  rc = gsasl_step64( session, "", &p );
  if ( ( rc != GSASL_OK ) && ( rc != GSASL_NEEDS_MORE ) ) { 
    croak( "MongoDB: No data from GSSAPI. Did you run kinit?\n" );
  }

  if ( ! strncpy( out_buf, p, 8192 ) ) {
    croak( "MongoDB: Unable to copy SASL output buffer\n" );
  }
  gsasl_free( p );

  result = (HV *)SvRV( perl_mongo_call_method( client, "_sasl_start", 0, 2, newSVpv( out_buf, 0 ), mechanism ) );

#if 0  
  fprintf( stderr, "result conv id = [%s]\n", SvPV_nolen( *hv_fetch( result, "conversationId", 14, FALSE ) ) );
  fprintf( stderr, "result payload = [%s]\n", SvPV_nolen( *hv_fetch( result, "payload",         7, FALSE ) ) );
#endif

  buf = SvPV_nolen( *hv_fetch( result, "payload", 7, FALSE ) );
  conv_id = *hv_fetch( result, "conversationId", 14, FALSE ); 
 
  do { 
    rc = gsasl_step64( session, buf, &p );
    if ( ( rc != GSASL_OK ) && ( rc != GSASL_NEEDS_MORE ) ) {
      croak( "MongoDB: SASL step error (%d): %s\n", rc, gsasl_strerror(rc) );
    }

    if ( ! strncpy( out_buf, p, 8192 ) ) { 
      croak( "MongoDB: Unable to copy SASL output buffer\n" );
    }
    gsasl_free( p );

    result = (HV *)SvRV( perl_mongo_call_method( client, "_sasl_continue", 0, 2, newSVpv( out_buf, 0 ), conv_id ) );
#if 0 
    fprintf( stderr, "result conv id = [%s]\n", SvPV_nolen( *hv_fetch( result, "conversationId", 14, FALSE ) ) );
    fprintf( stderr, "result payload = [%s]\n", SvPV_nolen( *hv_fetch( result, "payload",         7, FALSE ) ) );
#endif

    buf = SvPV_nolen( *hv_fetch( result, "payload", 7, FALSE ) );

  } while( rc == GSASL_NEEDS_MORE );

  if ( rc != GSASL_OK ) { 
    croak( "MongoDB: SASL Authentication error (%d): %s\n", rc, gsasl_strerror(rc) );
  }

  gsasl_finish( session );
  gsasl_done( ctx );
}
#endif  /* MONGO_SASL */

int perl_mongo_connect(SV *client, mongo_link* link) {
  SV* sasl_flag;
  int error;

#ifdef MONGO_SSL
  if(link->ssl){
    ssl_connect(link);
    link->sender = ssl_send;
    link->receiver = ssl_recv;
    return;
  }
#endif

  if ( (error = non_ssl_connect(link)) ) {
      return error;
  };
  link->sender = non_ssl_send;
  link->receiver = non_ssl_recv;

  sasl_flag = perl_mongo_call_method( client, "sasl", 0, 0 );

  if ( link->master->connected && SvIV(sasl_flag) == 1 ) {
#ifdef MONGO_SASL
      sasl_authenticate( client, link );
#else
      croak( "MongoDB: sasl => 1 specified, but this driver was not compiled with SASL support\n" );
#endif
  }
  
  SvREFCNT_dec(sasl_flag);
  
  return 0;
}

/*
 * Returns 0 on successful connection or timeout, positive errno on network
 * failure and negative h_errno on hostname resolution failure; callers should
 * check the link 'connected' field to see if 0 means connection or timeout
 */

int non_ssl_connect(mongo_link* link) {
  int sock, status, connected = 0;
  struct sockaddr_in addr;
  int error;

#ifdef WIN32
  WORD version;
  WSADATA wsaData;
  u_long no = 0;
  const char yes = 1;

  version = MAKEWORD(2,2);
  error = WSAStartup(version, &wsaData);

  if (error != 0) {
    return error;
  }

  // create socket
  sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sock == INVALID_SOCKET) {
    return WSAGetLastError();
  }

#else
  int yes = 1;

  // create socket
  if ((sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1) {
    return errno;
  }
#endif

  // get addresses
  if ((error = mongo_link_sockaddr(&addr, link->master->host, link->master->port))) {
#ifdef WIN32
    closesocket(link->master->socket);
#else
    close(sock);
#endif
    return -error; // h_error
  }

  setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &yes, INT_32);
  setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &yes, INT_32);

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
#ifdef WIN32
        closesocket(link->master->socket);
#else
        close(sock);
#endif
      return errno;
    }

    if ((error = mongo_link_timeout(sock, link->timeout))) {
#ifdef WIN32
        closesocket(link->master->socket);
#else
        close(sock);
#endif
      if ( error == -1 ) {
          return 0; // timeout
      }
      return error;
    }

    size = sizeof(addr);

    /* if connection failed, getpeername will fail and we can get
     * the original error via read. See http://cr.yp.to/docs/connect.html
     */

    connected = getpeername(sock, (struct sockaddr*)&addr, &size);
    if (connected == -1){
        char ch;
        read(sock,&ch,1); /* retrieve error */
        error = errno;
#ifdef WIN32
        closesocket(link->master->socket);
#else
        close(sock);
#endif
        return error;
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
  link->master->socket = sock;
  link->master->connected = 1;

  return 0;
}

#ifdef MONGO_SSL
// Establish a connection using an SSL layer
void ssl_connect(mongo_link* link) {
  tcp_setup(link);

  if (link->master->socket){
    // Register the error strings for libcrypto & libssl
    SSL_load_error_strings();

    // Register the available ciphers and digests
    SSL_library_init();

    // New context saying we are a client, and using SSL 2 or 3
    link->ssl_context = SSL_CTX_new(SSLv23_client_method());
    if(link->ssl_context == NULL){
      ERR_print_errors_fp(stderr);
    }

    // Create an SSL struct for the connection
    link->ssl_handle = SSL_new(link->ssl_context);
    if(link->ssl_handle == NULL){
      ERR_print_errors_fp(stderr);
    }

    // Connect the SSL struct to our connection
    if(!SSL_set_fd(link->ssl_handle, link->master->socket)){
      ERR_print_errors_fp(stderr);
    }

    // Initiate SSL handshake
    if(SSL_connect (link->ssl_handle) != 1){
      ERR_print_errors_fp(stderr);
    }

    SSL_CTX_set_timeout(link->ssl_context, (long)link->timeout);

    link->master->connected = 1;
  }
}

int ssl_send(void* link, const char* buffer, size_t len){
  return SSL_write(((mongo_link*)link)->ssl_handle, buffer, len);
}

int ssl_recv(void* link, const char* buffer, size_t len){
  return SSL_read(((mongo_link*)link)->ssl_handle, (void*)buffer, len);
}
#endif

int non_ssl_send(void* link, const char* buffer, size_t len){
  return send(((mongo_link*)link)->master->socket, buffer, len, 0);
}

int non_ssl_recv(void* link, const char* buffer, size_t len){
  return recv(((mongo_link*)link)->master->socket, (void*)buffer, len, 0);
}

static int mongo_link_timeout(int sock, time_t to) {
  struct timeval timeout, start, end, now, *timeptr;

  if (to >= 0) {
    timeout.tv_sec = (long)to / 1000;
    timeout.tv_usec = (to % 1000) * 1000;
    /* record max end time, in case we get interrupted and
     * have to recalculate the remaining timeout;
     * gettimeofday() isn't guaranteed monotonic, but it's
     * portable and only matters for EINTR */
    if (gettimeofday(&start, 0) == -1) {
      croak("Error: %s", strerror(errno));
    }
    timeval_add(&end, &start, &timeout);
    timeptr = &timeout;
  }
  else {
    /* block indefinitely */
    timeptr = NULL;
  }

  while (1) {
    fd_set rset, wset;
    int sock_status;

    FD_ZERO(&rset);
    FD_SET(sock, &rset);
    FD_ZERO(&wset);
    FD_SET(sock, &wset);

    sock_status = select(sock+1, &rset, &wset, NULL, timeptr);

    // error
    if (sock_status == -1) {

#ifdef WIN32
      errno = WSAGetLastError();
#endif

      if (errno == EINTR) {
        if (to >= 0) {
          if (gettimeofday(&now, 0) == -1) {
            croak("Error: %s", strerror(errno));
          }
          // update timeout; but timeout expired if winds up negative
          if ( timeval_subtract(&timeout, &end, &now) ) {
            return -1;
          }
        }
      }
      else {
        // if this isn't a EINTR, it's a fatal error
        return errno;
      }
    }
    else if (sock_status == 0) {
      return -1; // timeout expired
    }
    else {
      return 0;
    }
  }
}

static int mongo_link_sockaddr(struct sockaddr_in *addr, char *host, int port) {
  struct hostent *hostinfo;

  addr->sin_family = AF_INET;
  addr->sin_port = htons(port);
  hostinfo = (struct hostent*)gethostbyname(host);

  if (!hostinfo) {
#ifdef WIN32
    return WSAGetLastError();
#else
    return h_errno;
#endif
  }

#ifdef WIN32
  addr->sin_addr.s_addr = ((struct in_addr*)(hostinfo->h_addr))->s_addr;
#else
  addr->sin_addr = *((struct in_addr*)hostinfo->h_addr);
#endif

  return 0;
}


/*
 * Sends a message to the MongoDB server
 */
int mongo_link_say(SV *link_sv, buffer *buf) {
  int sock, sent;
  mongo_link *link;
  link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_sv, &connection_vtbl);

  if ((sock = perl_mongo_master(link_sv, 1)) == -1) {
    return -1;
  }

  sent = link->sender(link, (const char*)buf->start, buf->pos-buf->start);

  if (sent == -1) {
    set_disconnected(link_sv);
  }

  return sent;
}


static int get_header(int sock, SV *cursor_sv, SV *link_sv) {
  mongo_cursor *cursor;
  mongo_link *link;
  int size;

  cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(cursor_sv, &cursor_vtbl);
  link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_sv, &connection_vtbl);
  size = 0;

  size = link->receiver(link, (char*)&cursor->header.length, INT_32);
  if(size != INT_32){
    set_disconnected(link_sv);
    return 0;
  }

  cursor->header.length = MONGO_32(cursor->header.length);

  // make sure we're not getting crazy data
  if (cursor->header.length > MAX_RESPONSE_LEN || cursor->header.length < REPLY_HEADER_SIZE) {
    set_disconnected(link_sv);
    return 0;
  }

  if (link->receiver(link, (char*)&cursor->header.request_id, INT_32)  != INT_32  ||
      link->receiver(link, (char*)&cursor->header.response_to, INT_32) != INT_32  ||
      link->receiver(link, (char*)&cursor->header.op, INT_32)          != INT_32) {
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
  int sock;
  int num_returned = 0, timeout = -1;
  mongo_cursor *cursor;
  mongo_link *link;
  SV *link_sv, *request_id_sv, *timeout_sv;

  cursor = (mongo_cursor*)perl_mongo_get_ptr_from_instance(cursor_sv, &cursor_vtbl);
  link_sv = perl_mongo_call_reader(cursor_sv, "_client");
  link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_sv, &connection_vtbl);
  timeout_sv = perl_mongo_call_reader(link_sv, "query_timeout");

  if ((sock = perl_mongo_master(link_sv, 0)) == -1) {
    set_disconnected(link_sv);
    SvREFCNT_dec(link_sv);
    croak("can't get db response, not connected (during receive)");
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
    FD_SET(sock, &readfds);

    select(sock+1, &readfds, NULL, NULL, &t);

    if (!FD_ISSET(sock, &readfds)) {
      SvREFCNT_dec(link_sv);
      croak("recv timed out (%d ms)", timeout);
      return 0;
    }
  }

  if (get_header(sock, cursor_sv, link_sv) == 0) {
    SvREFCNT_dec(link_sv);
    croak("can't get db response, not connected (invalid response header)");
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

    if (link->receiver(link, (char*)temp, 20) == -1) {
      SvREFCNT_dec(link_sv);
      SvREFCNT_dec(request_id_sv);
      croak("couldn't get header response to throw out");
      return 0;
    }

    do {
      int temp_len = len > 4096 ? 4096 : len;
      len -= temp_len;

      if (mongo_link_reader(link, (void*)temp, temp_len) == -1) {
        SvREFCNT_dec(link_sv);
        SvREFCNT_dec(request_id_sv);
        croak("couldn't get response to throw out");
        return 0;
      }
    } while (len > 0);

    if (get_header(sock, cursor_sv, link_sv) == 0) {
      SvREFCNT_dec(link_sv);
      SvREFCNT_dec(request_id_sv);
      croak("invalid header received");
      return 0;
    }
  }
  SvREFCNT_dec(request_id_sv);

  if (link->receiver(link, (char*)&cursor->flag, INT_32)      == -1 ||
      link->receiver(link, (char*)&cursor->cursor_id, INT_64) == -1 ||
      link->receiver(link, (char*)&cursor->start, INT_32)     == -1 ||
      link->receiver(link, (char*)&num_returned, INT_32)      == -1) {
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

  if (mongo_link_reader(link, cursor->buf.pos, cursor->header.length) == -1) {
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
static int mongo_link_reader(mongo_link* link, void *dest, int len) {
  int num = 1, read = 0;

  // this can return FAILED if there is just no more data from db
  while (read < len && num > 0) {
    int temp_len = (len - read) > 4096 ? 4096 : (len - read);

    // windows gives a WSAEFAULT if you try to get more bytes
    num = link->receiver(link, (char*)dest, temp_len);

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
  if (link->master == 0 || link->master->connected == 0) {
      return;
  }

#ifdef WIN32
  shutdown(link->master->socket, 2);
  closesocket(link->master->socket);
  /* this might be a bug -- we should defer this to program exit or get the Perl
   * interpreter to do it */
  WSACleanup();
#else
  close(link->master->socket);
#endif

#ifdef MONGO_SSL
  if(link->ssl){
    ssl_disconnect(link);
  }
#endif

  link->master->connected = 0;

  // TODO: set $self->_master to 0?
  if (link->copy) {
      link->master = 0;
      perl_mongo_call_method(link_sv, "_master", G_DISCARD, 1, &PL_sv_no);
  }
}

int perl_mongo_master(SV *link_sv, int auto_reconnect) {
  SV *master;
  mongo_link *link;

  link = (mongo_link*)perl_mongo_get_ptr_from_instance(link_sv, &connection_vtbl);

  if (link->master && link->master->connected) {
      return link->master->socket;
  }
  // if we didn't have a connection above and this isn't a connection holder
  if (!link->copy) {
      // if this is a real connection, try to reconnect
      if (auto_reconnect && link->auto_reconnect) {
          perl_mongo_call_method(link_sv, "connect", G_DISCARD, 0);
          if (link->master && link->master->connected) {
              return link->master->socket;
          }
      }

      return -1;
  }

  // re-initialize master
  link->master = 0;
  master = perl_mongo_call_method(link_sv, "get_master", 0, 0);
  if (SvROK(master)) {
    mongo_link *m_link;

    m_link = (mongo_link*)perl_mongo_get_ptr_from_instance(master, &connection_vtbl);
    link->copy = 1;
    link->master = m_link->master;
    link->ssl = m_link->ssl;
#ifdef MONGO_SSL
    link->ssl_handle = m_link->ssl_handle;
    link->ssl_context = m_link->ssl_context;
#endif
    link->sender = m_link->sender;
    link->receiver = m_link->receiver;

    return link->master->socket;
  }

  return -1;
}

#ifdef MONGO_SSL
// Establish a regular tcp connection
void tcp_setup(mongo_link* link){
  int error, handle;
  struct hostent *host;
  struct sockaddr_in server;

  host = gethostbyname (link->master->host);
  handle = socket (AF_INET, SOCK_STREAM, 0);
  if (handle == -1){
    handle = 0;
  }
  else {
    server.sin_family = AF_INET;
    server.sin_port = htons (link->master->port);
    server.sin_addr = *((struct in_addr *) host->h_addr);
    bzero (&(server.sin_zero), 8);

    error = connect(handle, (struct sockaddr *) &server, sizeof (struct sockaddr));
    if (error == -1){
      handle = 0;
    }
  }

  link->master->socket = handle;
}

// Disconnect & free connection struct
void ssl_disconnect (mongo_link *link){
  if(link->ssl_handle){
    SSL_shutdown (link->ssl_handle);
    SSL_free (link->ssl_handle);
  }

  if (link->ssl_context)
    SSL_CTX_free (link->ssl_context);
}
#endif
