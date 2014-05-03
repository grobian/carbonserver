VERSION=0.6
distdir=carbonserver-$(VERSION)

carbonserver:
	GOPATH=`pwd`/Godeps/_workspace go build -o carbonserver

dist:
	godep save
	mkdir $(distdir)
	mv Godeps $(distdir)
	cp Makefile *.go $(distdir)
	tar zvcf $(distdir).tar.gz $(distdir)
	rm -rf $(distdir)

clean:
	rm -rf carbonserver $(distdir).tar.gz
