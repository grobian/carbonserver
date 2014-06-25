VERSION=0.9
distdir=carbonserver-$(VERSION)

carbonserver:
	GOPATH=`pwd` go build github.com/grobian/carbonserver

dist:
	mkdir -p GOPATH
	GOPATH=`pwd`/GOPATH go get -v -d
	git archive --prefix=github.com/grobian/carbonserver/ master |tar xv -C GOPATH/src
	cp Makefile GOPATH
	mv GOPATH $(distdir)
	tar zvcf $(distdir).tar.gz $(distdir)
	rm -rf $(distdir)

clean:
	rm -rf carbonserver $(distdir).tar.gz
