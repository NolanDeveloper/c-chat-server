/* Protocol:

setting nickname
    c> my name is <nick>
    s> ok

watching participants
    c> folks
    s> 3
    s> <nick>
    s> <nick1>
    s> <nick2>

sending messages
    c> send <message>
    s> ok

requesting new messages
    c> new
    s> 1
    s> [03:14:48] <nick2>: <message1>

    c> new
    s> 0
*/

#define _POSIX_C_SOURCE 200112L

#include <arpa/inet.h>
#include <poll.h>
#include <unistd.h>

#include <assert.h>
#include <time.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MIN(a, b) ((a) < (b) ? (a) : (b))

#define TIMESTAMP_LENGTH   10
#define BUFFER_POOL_SIZE   16
#define MAX_CONNECTIONS    1024
#define MAX_MESSAGE_LENGTH 140
#define MAX_NICK_LENGTH    20
#define MAX_HISTORY_LENGTH 50
#define MAX_PACKAGE_LENGTH \
  (TIMESTAMP_LENGTH + MAX_NICK_LENGTH + MAX_MESSAGE_LENGTH + 3)

struct Buffer {
  char data[MAX_PACKAGE_LENGTH];
  int used;
};

struct LinkedBuffer {
  struct Buffer buffer;
  struct LinkedBuffer * next;
};

struct ListOfBuffers {
  struct LinkedBuffer * first;
  struct LinkedBuffer * last;
};

struct LinkedBuffer free_buffers[BUFFER_POOL_SIZE];
struct LinkedBuffer * first_free_buffer;

struct ConnectionData {
  unsigned closed : 1;
  char nick[MAX_NICK_LENGTH + 1];
  struct timespec last_received_message;
  struct ListOfBuffers pending_to_be_sent;
  struct Buffer input_buffer;
};

static struct {
  struct pollfd sockets[MAX_CONNECTIONS];
  struct ConnectionData data[MAX_CONNECTIONS];
  int length;
} connections;

struct Message {
  char nick[MAX_NICK_LENGTH + 1];
  char data[MAX_MESSAGE_LENGTH + 1];
  struct timespec time;
};

static struct {
  struct Message messages[MAX_HISTORY_LENGTH];
  int length;
} history;

static char *
system_error(void) { return strerror(errno); }

static void
die(const char * fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  putchar('\n');
  exit(EXIT_FAILURE);
}

static struct timespec
get_time(void) {
  struct timespec time;
  if (-1 == clock_gettime(CLOCK_REALTIME, &time)) {
    die("'clock_gettime' failed: %s", system_error());
  }
  return time;
}

static void
show_usage(char * program) { die("usage: %s <port>", program); }

static void
prepare_server(uint16_t port) {
  int error, t;
  static struct sockaddr_in server;
  int server_fd;
  server_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (-1 == server_fd) die("'socket' failed: %s", system_error());
  server.sin_family = AF_INET;
  server.sin_port = htons(port);
  server.sin_addr.s_addr = htonl(INADDR_ANY);
  t = 1;
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &t, sizeof(t));
  error = bind(server_fd, (struct sockaddr *) &server, sizeof(server));
  if (-1 == error) die("'bind' failed: %s", system_error());
  error = listen(server_fd, 128);
  if (-1 == error) die("'listen' failed: %s", system_error());
  connections.sockets[0].fd = server_fd;
  connections.sockets[0].events = POLLIN;
  connections.length = 1;
}

static struct LinkedBuffer *
take_buffer(void) {
  struct LinkedBuffer * buffer;
  buffer = first_free_buffer;
  if (!buffer) die("Memory limit exceeded");
  first_free_buffer = first_free_buffer->next;
  buffer->buffer.used = 0;
  buffer->next = NULL;
  return buffer;
}

static void
release_buffer(struct LinkedBuffer * buffer) {
  buffer->next = first_free_buffer;
  first_free_buffer = buffer;
}

static void
send_later(int client, char * message) {
  struct ListOfBuffers * pending;
  struct Buffer * buffer;
  size_t size, stored, part_size;
  size = strlen(message);
  pending = &connections.data[client].pending_to_be_sent;
  if (!pending->last) {
    pending->last = pending->first = take_buffer();
  } else if (pending->last->buffer.used == sizeof(pending->last->buffer.data)) {
    pending->last->next = take_buffer();
    pending->last = pending->last->next;
  }
  stored = 0;
  while (stored < size) {
    if (stored) {
      pending->last->next = take_buffer();
      pending->last = pending->last->next;
    }
    buffer = &pending->last->buffer;
    part_size = MIN(sizeof(buffer->data) - buffer->used, size - stored);
    memcpy(buffer->data + buffer->used, message + stored, part_size);
    buffer->used += part_size;
    stored += part_size;
  }
  connections.sockets[client].events = POLLOUT;
}

static void
send_package(int client, char * message) {
  printf("send_package(%d, %s)\n", client, message);
  send_later(client, message);
  send_later(client, "\r\n");
}

static int
starts_with(char * string, const char * start) {
  return !strncmp(string, start, strlen(start));
}

static void
add_to_history(char * nick, char * message) {
  struct timespec now;
  printf("add_to_history(%s)\n", message);
  now = get_time();
  memmove(history.messages + 1, history.messages,
    history.length * sizeof(history.messages[0]));
  strcpy(history.messages[0].nick, nick);
  strcpy(history.messages[0].data, message);
  history.messages[0].time = now;
  if (history.length < MAX_HISTORY_LENGTH) ++history.length;
}

#define PACKAGE_BEGIN_MY_NAME_IS "my name is "
#define PACKAGE_BEGIN_SEND "send "
#define PACKAGE_OK "ok"
#define PACKAGE_FOLKS "folks"
#define PACKAGE_NEW "new"

static int
older(struct timespec a, struct timespec b) {
  return a.tv_sec < b.tv_sec ||
    (a.tv_sec == b.tv_sec && a.tv_nsec < b.tv_nsec);
}

static int
process_new_package(int client, char * package) {
  int length, i;
  char outgoing[MAX_PACKAGE_LENGTH];
  struct timespec last_received_message, time;
  struct tm pretty_time;
  printf("process_new_package(%d, %s)\n", client, package);
  if (starts_with(package, PACKAGE_BEGIN_MY_NAME_IS)) {
    length = strlen(package + strlen(PACKAGE_BEGIN_MY_NAME_IS));
    if (MAX_NICK_LENGTH < length) goto close_connection;
    strcpy(connections.data[client].nick,
      package + strlen(PACKAGE_BEGIN_MY_NAME_IS));
    send_package(client, PACKAGE_OK);
  } else if (!strcmp(package, PACKAGE_FOLKS)) {
    sprintf(outgoing, "%d", connections.length - 1);
    send_package(client, outgoing);
    for (i = 1; i < connections.length; ++i) {
      send_package(client, connections.data[i].nick);
    }
  } else if (starts_with(package, PACKAGE_BEGIN_SEND)) {
    length = strlen(package + strlen(PACKAGE_BEGIN_SEND));
    if (MAX_MESSAGE_LENGTH < length) return -1;
    add_to_history(connections.data[client].nick,
      package + strlen(PACKAGE_BEGIN_SEND));
    send_package(client, PACKAGE_OK);
  } else if (!strcmp(package, PACKAGE_NEW)) {
    last_received_message = connections.data[client].last_received_message;
    i = history.length - 1;
    while (older(history.messages[i].time, last_received_message) && i >= 0) {
      --i;
    }
    sprintf(outgoing, "%d", i + 1);
    send_package(client, outgoing);
    while (i >= 0) {
      time = history.messages[i].time;
      if (!localtime_r(&time.tv_sec, &pretty_time)) {
        goto close_connection;
      }
      sprintf(outgoing, "[%02d:%02d:%02d] %s: %s",
        pretty_time.tm_hour, pretty_time.tm_min, pretty_time.tm_sec,
        history.messages[i].nick, history.messages[i].data);
      send_package(client, outgoing);
      --i;
    }
    connections.data[client].last_received_message = get_time();
  } else goto close_connection;
  return 0;
close_connection:
  close(connections.sockets[client].fd);
  return -1;
}

static int
process_new_data(int client) {
  struct Buffer * buffer;
  char * begin, * end_of_package;
  printf("process_new_data(%d)\n", client);
  buffer = &connections.data[client].input_buffer;
  begin = buffer->data;
  while (1) {
    end_of_package = strstr(begin, "\r\n");
    if (!end_of_package && begin == buffer->data &&
        sizeof(buffer->data) - 1 == buffer->used) {
      printf("Too long message. Connection was closed.\n");
      close(connections.sockets[client].fd);
      return -1;
    }
    if (!end_of_package) break;
    *end_of_package = '\0';
    if (-1 == process_new_package(client, begin)) return -1;
    begin = end_of_package + 2;
  }
  buffer->used -= begin - buffer->data;
  memmove(buffer->data, begin, buffer->used);
  return 0;
}

static void
handle_input(int client) {
  struct Buffer * buffer;
  int fd;
  ssize_t received;
  printf("handle_input(%d)\n", client);
  fd = connections.sockets[client].fd;
  buffer = &connections.data[client].input_buffer;
  received = recv(fd, buffer->data + buffer->used,
    sizeof(buffer->data) - buffer->used - 1, 0);
  if (!received) goto close_connection;
  if (-1 == received) {
    printf("'recv' failed: %s\n", system_error());
    goto close_connection;
  }
  buffer->used += received;
  buffer->data[buffer->used] = '\0';
  if (-1 == process_new_data(client)) goto close_connection;
  return;
close_connection:
  connections.data[client].closed = 1;
}

static void
handle_output(int client) {
  struct LinkedBuffer * buffer;
  struct ListOfBuffers * pending;
  int fd;
  ssize_t sent;
  printf("handle_output(%d)\n", client);
  pending = &connections.data[client].pending_to_be_sent;
  fd = connections.sockets[client].fd;
  assert(pending->first);
  while (1) {
    buffer = pending->first;
    sent = send(fd, buffer->buffer.data, buffer->buffer.used, 0);
    if (-1 == sent) {
      if (EWOULDBLOCK == errno || EAGAIN == errno) return;
      die("'send' failed: %s", system_error());
    }
    assert(sent == buffer->buffer.used);
    pending->first = buffer->next;
    release_buffer(buffer);
    if (!pending->first) {
      pending->last = NULL;
      break;
    }
  }
  connections.sockets[client].events = POLLIN;
}

static void
init() {
  int i, n;
  for (i = 0, n = sizeof(free_buffers) / sizeof(free_buffers[0]) - 1;
      i < n; ++i) {
    free_buffers[i].next = &free_buffers[i + 1];
  }
  first_free_buffer = &free_buffers[0];
}

static void
accept_new_client(void) {
  struct sockaddr_in address;
  socklen_t address_length;
  int client_fd, n;
  printf("accept_new_client()\n");
  address_length = sizeof(address);
  client_fd = accept(connections.sockets[0].fd,
    (struct sockaddr *) &address, &address_length);
  if (client_fd < 0) die("'accept' failed: %s", system_error());
  n = connections.length;
  connections.sockets[n].fd = client_fd;
  connections.sockets[n].events = POLLIN;
  memset(&connections.data[n], 0, sizeof(connections.data[n]));
  strcpy(connections.data[n].nick, "anonym");
  connections.data[n].last_received_message = get_time();
  ++connections.length;
}

static void
clean_closed_sockets(void) {
  int s, d;
  for (s = d = 0; s < connections.length; ++s) {
    if (connections.data[s].closed) continue;
    connections.sockets[d] = connections.sockets[s];
    connections.data[d] = connections.data[s];
    ++d;
  }
  connections.length = d;
}

int
main(int argc, char * argv[]) {
  int i, n;
  char * end;
  unsigned long port;
  short events;
  init();
  if (2 != argc) show_usage(argv[0]);
  port = strtoul(argv[1], &end, 10);
  if (*end) show_usage(argv[0]);
  if (!port) die("port 0 is not allowed");
  if (65535 < port) die("port is too big");
  prepare_server(port);
  while (1) {
    printf("polling...\n");
    if (-1 == poll(connections.sockets, connections.length, -1)) {
      die("'poll' failed: %s", system_error());
    }
    printf("new event\n");
    for (i = 0, n = connections.length; i < n; ++i) {
      events = connections.sockets[i].revents;
      if (events & POLLIN) {
        if (!i) accept_new_client();
        else handle_input(i);
      } if (events & POLLOUT) {
        assert(i);
        handle_output(i);
      } else if (events & (POLLERR | POLLHUP | POLLNVAL)) {
        connections.data[i].closed = 1;
      }
    }
    clean_closed_sockets();
  }
  return 0;
}
