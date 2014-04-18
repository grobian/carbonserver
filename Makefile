VERSION=0.1
distdir=carbonserver-$(VERSION)

carbonserver:
	GOPATH=`pwd` go build -o $@

dist:
	mkdir -p $(distdir)
	git archive \
		--format=tar.gz \
		--prefix=$(distdir)/ v$(VERSION) \
		| tar -zxf -
	cp -a src $(distdir)/
	tar -zcf $(distdir).tar.gz $(distdir)

clean:
	rm -rf carbonserver $(distdir)
