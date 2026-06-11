BINARY   := dns-cache
PREFIX   := /usr/local
CONFIG   := /etc/dns-cache.yaml
DBDIR    := /var/cache/dns-cache
SOCKDIR  := /var/run
UNIT     := /etc/systemd/system/dns-cache.service

.PHONY: all build install uninstall clean

all: build

build:
	go build -o $(BINARY) -ldflags="-s -w" .

install: build
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 755 $(BINARY) $(DESTDIR)$(PREFIX)/bin/$(BINARY)
	install -d $(DESTDIR)$(DBDIR)
	install -d $(DESTDIR)$(SOCKDIR)
	install -m 644 dns-cache.yaml.example $(DESTDIR)$(CONFIG)
	install -m 644 dns-cache.service $(DESTDIR)$(UNIT)
	systemctl daemon-reload

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/$(BINARY)
	rm -f $(DESTDIR)$(UNIT)
	systemctl daemon-reload

clean:
	rm -f $(BINARY)
