# Makefile for mod_socket_audio
# Standalone build without full FreeSWITCH source tree

MODULE_NAME = mod_socket_audio
MODULE_SO = $(MODULE_NAME).so

# FreeSWITCH paths
# Detect module directory (system package vs local install)
FS_MOD_DIR ?= $(shell fs_cli -x "global_getvar mod_dir" 2>/dev/null || echo "/usr/lib/freeswitch/mod")

# Compiler settings
CC = gcc
CFLAGS = -fPIC -g -O2 -Wall -Wextra -Wno-unused-parameter
CFLAGS += $(shell pkg-config --cflags freeswitch 2>/dev/null || echo "-I/usr/include/freeswitch -I$(FS_PREFIX)/include/freeswitch")
LDFLAGS = -shared
LIBS = $(shell pkg-config --libs freeswitch 2>/dev/null || echo "-lfreeswitch")

# Source files
SRCS = mod_socket_audio.c
OBJS = $(SRCS:.c=.o)

.PHONY: all clean install uninstall reload

all: $(MODULE_SO)

$(MODULE_SO): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJS) $(MODULE_SO)

install: $(MODULE_SO)
	install -m 0755 $(MODULE_SO) $(FS_MOD_DIR)/
	@echo ""
	@echo "Installed $(MODULE_SO) to $(FS_MOD_DIR)"
	@echo ""
	@echo "To load the module, run in fs_cli:"
	@echo "  load mod_socket_audio"
	@echo ""
	@echo "Or add to modules.conf.xml:"
	@echo "  <load module=\"mod_socket_audio\"/>"
	@echo ""

uninstall:
	rm -f $(FS_MOD_DIR)/$(MODULE_SO)
	@echo "Uninstalled $(MODULE_SO)"

# Reload module in running FreeSWITCH (useful during development)
reload: install
	@echo "Reloading module in FreeSWITCH..."
	fs_cli -x "reload mod_socket_audio" 2>/dev/null || \
	fs_cli -x "load mod_socket_audio" 2>/dev/null || \
	echo "Note: Could not reload. Start FreeSWITCH and load manually."
