VERSION=0.6
distdir=carbonserver-$(VERSION)

carbonserver: Godeps
	GOPATH=`pwd`/Godeps go build -o $@

dist: Godeps
	godep save
	mkdir $(distdir)
	mv Godeps $(distdir)
	cp Makefile *.go $(distdir)
	tar zvcf $(distdir).tar.gz $(distdir)
	rm -rf $(distdir)

Godeps:
	mkdir -p Godeps
	GOPATH=`pwd`/Godeps go get -d

clean:
	rm -rf carbonserver $(distdir).tar.gz
