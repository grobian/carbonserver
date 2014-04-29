VERSION=0.4
distdir=carbonserver-$(VERSION)

carbonserver:
	GOPATH=`pwd` go build -o $@

dist:
	mkdir -p $(distdir)
	git archive \
		--format=tar.gz \
		--prefix=$(distdir)/ v$(VERSION) \
		| tar -zxf -
	rsync -Ca src $(distdir)/
	tar -zcf $(distdir).tar.gz $(distdir)
	rm -rf $(distdir)

clean:
	rm -rf carbonserver $(distdir).tar.gz
