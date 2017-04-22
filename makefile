CFLAGS += -g -std=c89 -Wall -Wextra -Werror -pedantic -fmax-errors=1

.PHONY: clean

server: server.o

clean:
	$(RM) server.o server
