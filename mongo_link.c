#include "mongo_link.h"

static int mongo_link_sockaddr(struct sockaddr_in *addr, char *host, int port);
static int mongo_link_reader(int socket, void *dest, int len);
static int do_connect(char *host, int port);
static int check_connection(mongo_link *link);
static int get_master(mongo_link *link);

int mongo_link_connect(mongo_link *link) {
  if (link->paired) {
    link->server.pair.left_socket = do_connect(link->server.pair.left_host, link->server.pair.left_port);
    link->server.pair.right_socket = do_connect(link->server.pair.right_host, link->server.pair.right_port);
    return link->server.pair.left_socket && link->server.pair.right_socket;
  }

  link->server.single.socket = do_connect(link->server.single.host, link->server.single.port);
  return link->server.single.socket;
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
int mongo_link_say(mongo_link *link, buffer *buf) {
  int sock, sent;

  sock = get_master(link);
  sent = send(sock, (const char*)buf->start, buf->pos-buf->start, 0);

  if (sent == -1) {
    if (check_connection(link)) {
      sock = get_master(link);
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
int mongo_link_hear(mongo_link *link, mongo_cursor *cursor) {
  int sock = get_master(link);
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
  int now;
#ifdef WIN32
  SYSTEMTIME systemTime;
  GetSystemTime(&systemTime);
  now = systemTime.wMilliseconds;
#else
  now = time(0);
#endif

  if (!link->auto_reconnect ||
      (now-link->ts) < 2) {
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

  return mongo_link_connect(link);
}

static int get_master(mongo_link *link) {
  if (!link->paired) {
    return link->server.single.socket;
  }

  if (link->server.pair.left_socket == link->master) {
    return link->server.pair.left_socket;
  }
  else if (link->server.pair.right_socket == link->master) {
    return link->server.pair.right_socket;
  }

  return -1;
  /*
  MAKE_STD_ZVAL(cursor_zval);
  object_init_ex(cursor_zval, mongo_ce_Cursor);
  cursor = (mongo_cursor*)zend_object_store_get_object(cursor_zval TSRMLS_CC);

  // redetermine master
  MAKE_STD_ZVAL(query);
  object_init(query);
  MAKE_STD_ZVAL(is_master);
  object_init(is_master);
  add_property_long(is_master, "ismaster", 1);
  add_property_zval(query, "query", is_master);

  cursor->ns = estrdup("admin.$cmd");
  cursor->query = query;
  cursor->fields = 0;
  cursor->limit = -1;
  cursor->skip = 0;
  cursor->opts = 0;

  temp.paired = 0;
  // check the left
  temp.server.single.socket = link->server.paired.lsocket;
  cursor->link = &temp;

  // need to call this after setting cursor->link
  // reset checks that cursor->link != 0
  MONGO_METHOD(MongoCursor, reset)(0, &temp_ret, NULL, cursor_zval, 0 TSRMLS_CC);

  MAKE_STD_ZVAL(response);
  MONGO_METHOD(MongoCursor, getNext)(0, response, NULL, cursor_zval, 0 TSRMLS_CC);
  if ((Z_TYPE_P(response) == IS_ARRAY ||
       Z_TYPE_P(response) == IS_OBJECT) &&
      zend_hash_find(HASH_P(response), "ismaster", 9, (void**)&ans) == SUCCESS &&
      Z_LVAL_PP(ans) == 1) {
    zval_ptr_dtor(&cursor_zval);
    zval_ptr_dtor(&query);
    zval_ptr_dtor(&response);
    return link->master = link->server.paired.lsocket;
  }

  // reset response
  zval_ptr_dtor(&response);
  MAKE_STD_ZVAL(response);

  // check the right
  temp.server.single.socket = link->server.paired.rsocket;
  cursor->link = &temp;

  MONGO_METHOD(MongoCursor, reset)(0, &temp_ret, NULL, cursor_zval, 0 TSRMLS_CC);
  MONGO_METHOD(MongoCursor, getNext)(0, response, NULL, cursor_zval, 0 TSRMLS_CC);
  if ((Z_TYPE_P(response) == IS_ARRAY ||
       Z_TYPE_P(response) == IS_OBJECT) &&
      zend_hash_find(HASH_P(response), "ismaster", 9, (void**)&ans) == SUCCESS &&
      Z_LVAL_PP(ans) == 1) {
    zval_ptr_dtor(&cursor_zval);
    zval_ptr_dtor(&query);
    zval_ptr_dtor(&response);
    return link->master = link->server.paired.rsocket;
  }

  zval_ptr_dtor(&response);
  zval_ptr_dtor(&query);
  zval_ptr_dtor(&cursor_zval);
  return FAILURE;
  */
}
