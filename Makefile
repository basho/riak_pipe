REPO		?= riak_pipe
RIAK_TAG	 = $(shell git describe --tags)
REVISION	?= $(shell echo $(RIAK_TAG) | sed -e 's/^$(REPO)-//')
PKG_VERSION	?= $(shell echo $(REVISION) | tr - .)
PULSE_TESTS	 = reduce_fitting_pulse

.PHONY: deps

all: deps compile

compile: deps
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

distclean: clean ballclean
	./rebar delete-deps

# You should 'clean' before your first run of this target
# so that deps get built with PULSE where needed.
pulse:
	./rebar compile -D PULSE
	./rebar eunit -D PULSE skip_deps=true suite=$(PULSE_TESTS)

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool snmp public_key mnesia eunit syntax_tools compiler

include tools.mk

# Release tarball creation
# Generates a tarball that includes all the deps sources so no checkouts are necessary
archivegit = git archive --format=tar --prefix=$(1)/ HEAD | (cd $(2) && tar xf -)
archivehg = hg archive $(2)/$(1)
archive = if [ -d ".git" ]; then \
		$(call archivegit,$(1),$(2)); \
	    else \
		$(call archivehg,$(1),$(2)); \
	    fi

buildtar = mkdir distdir && \
		 git clone . distdir/riak_pipe-clone && \
		 cd distdir/riak_pipe-clone && \
		 git checkout $(RIAK_TAG) && \
		 $(call archive,$(RIAK_TAG),..) && \
		 mkdir ../$(RIAK_TAG)/deps && \
		 make deps; \
		 for dep in deps/*; do \
                     cd $${dep} && \
                     $(call archive,$${dep},../../../$(RIAK_TAG)) && \
                     mkdir -p ../../../$(RIAK_TAG)/$${dep}/priv && \
                     git rev-list --max-count=1 HEAD > ../../../$(RIAK_TAG)/$${dep}/priv/git.vsn && \
                     cd ../..; done

distdir:
	$(if $(RIAK_TAG), $(call buildtar), $(error "You can't generate a release tarball from a non-tagged revision. Run 'git checkout <tag>', then 'make dist'"))

dist $(RIAK_TAG).tar.gz: distdir
	cd distdir; \
	tar czf ../$(RIAK_TAG).tar.gz $(RIAK_TAG)

ballclean:
	rm -rf $(RIAK_TAG).tar.gz distdir

package: dist
	$(MAKE) -C package package

pkgclean:
	$(MAKE) -C package pkgclean

.PHONY: package
export PKG_VERSION REPO REVISION RIAK_TAG
