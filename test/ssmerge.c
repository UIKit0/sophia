
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <time.h>
#include <sys/time.h>
#include <string.h>

#define _GNU_SOURCE 1
#include <stdlib.h>

#include <libsr.h>
#include <libsv.h>
#include <libsl.h>
#include <libsm.h>
#include <libss.h>
#include <sophia.h>
#include "test.h"

static inline void
confset(srconf *c, sra *a, ...)
{
	va_list args;
	va_start(args, a);
	char *path = va_arg(args, char*);
	t( sr_confset(c, a, path, args) == 0 );
	va_end(args);
}

static void
test_valloc(srbuf *buf, sra *a, uint64_t lsn, int key)
{
	svlocal l;
	memset(&l, 0, sizeof(l));
	l.lsn         = lsn,
	l.flags       = SVSET,
	l.key         = &key;
	l.keysize     = sizeof(key);
	l.value       = NULL;
	l.valuesize   = 0;
	l.valueoffset = 0;
	sv lv;
	svinit(&lv, &sv_localif, &l, NULL);
	smv *m = sm_valloc(a, &lv);
	t(m != NULL);
	sv mv;
	svinit(&mv, &sm_vif, m, NULL);
	t( sr_bufadd(buf, a, &mv, sizeof(sv)) == 0 );
}

static void
freebuf(srbuf *buf, sra *a)
{
	sriter stream;
	sr_iterinit(&stream, &sr_bufiter, NULL);
	sr_iteropen(&stream, buf, sizeof(sv));
	while (sr_iterhas(&stream)) {
		sv *v = (sv*)sr_iterof(&stream);
		sr_free(a, v->v);
		sr_iternext(&stream);
	}
	sr_buffree(buf, a);
}

static void validate_index(ss *s, src *c, ssindex *i)
{
	srbuf rbuf;
	sr_bufinit(&rbuf);
	sriter it;
	sr_iterinit(&it, &ss_indexiterraw, c);
	sr_iteropen(&it, i);

	int prevmin = 0;
	int prevmax = 0;
	int pagen = 0;

	while (sr_iterhas(&it))
	{
		ssref *page = sr_iterof(&it);
		sspage origin;
		t( ss_read(s, c, &rbuf, &origin, page, NULL) == 0 );

		t( *(int*)ss_refmin(page) == *(int*)ss_pagemin(&origin)->key );
		t( *(int*)ss_refmax(page) == *(int*)ss_pagemax(&origin)->key );
		t( page->lsnmin == origin.h->lsnmin );
		t( page->lsnmax == origin.h->lsnmax );

		if (pagen > 0) {
			t( *(int*)ss_refmin(page) > prevmin );

			t( prevmax < *(int*)ss_refmin(page) );
			t( prevmin < *(int*)ss_refmin(page) );
		}

		int count = 0;
		sriter j;
		sr_iterinit(&j, &ss_pageiterraw, c);
		sr_iteropen(&j, &origin);
		while (sr_iterhas(&j))
		{
			sv *vp = sr_iterof(&j);
			(void)vp;
			count++;
			sr_iternext(&j);
		}
		t( count == origin.h->count );

		sr_iternext(&it);
		pagen++;

		prevmin = *(int*)ss_pagemin(&origin)->key;
		prevmax = *(int*)ss_pagemax(&origin)->key;
	}

	t( pagen == i->n );
	sr_buffree(&rbuf, c->a);
}

static void
test_merge_empty(void)
{
	rmrf("./test");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	srseq seq;
	sr_seqinit(&seq);
	t( sr_confinit(&conf, &a) == 0 );
	confset(&conf, &a, "env.dir", "test");
	confset(&conf, &a, "env.page_size", 4);
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);
	ssc sc;
	ss_cinit(&sc, &c);
	ss store;
	ss_init(&store);
	t( ss_open(&store, &c) == 1 );

	ssindex i;
	ss_indexinit(&i, 0);

	srbuf src;
	sr_bufinit(&src);

	sriter stream;
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));

	ssmerge m;
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 0);
	t( ss_merge(&m) == 0 );

	t( m.b->n == 0 );
	t( m.result.n == 0 );

	ss_mergefree(&m);

	t( ss_close(&store, &c) == 0 );

	freebuf(&src, &a);
	ss_cfree(&sc, &c);
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_single(void)
{
	rmrf("./test");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	srseq seq;
	sr_seqinit(&seq);
	t( sr_confinit(&conf, &a) == 0 );
	confset(&conf, &a, "env.dir", "test");
	confset(&conf, &a, "env.page_size", 4);
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);
	ssc sc;
	ss_cinit(&sc, &c);
	ss store;
	ss_init(&store);
	t( ss_open(&store, &c) == 1 );

	ssindex i;
	ss_indexinit(&i, 0);

	srbuf src;
	sr_bufinit(&src);
	test_valloc(&src, &a, 2, 7);
	test_valloc(&src, &a, 3, 8);
	test_valloc(&src, &a, 4, 9);

	sriter stream;
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));

	ssmerge m;
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 0);
	t( ss_merge(&m) == 0 );
	t( m.b->n == 1 );
	t( m.result.n == 1 );

	ssref *p = ss_indexmin(&m.result);
	t( *(int*)ss_refmin(p) == 7 );
	t( *(int*)ss_refmax(p) == 9 );
	t( p->lsnmin == 2 );
	t( p->lsnmax == 4 );
	t( ss_indexmax(&m.result) == ss_indexmin(&m.result));

	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 2 );
	t( m.result.lsnmax == 4 );
	validate_index(&store, &c, &m.result);

	ss_mergefree(&m);

	t( ss_close(&store, &c) == 0 );

	freebuf(&src, &a);
	ss_cfree(&sc, &c);
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_single_remerge0(void)
{
	rmrf("./test");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	srseq seq;
	sr_seqinit(&seq);
	t( sr_confinit(&conf, &a) == 0 );
	confset(&conf, &a, "env.dir", "test");
	confset(&conf, &a, "env.page_size", 4);
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);
	ssc sc;
	ss_cinit(&sc, &c);
	ss store;
	ss_init(&store);
	t( ss_open(&store, &c) == 1 );

	ssindex i;
	ss_indexinit(&i, 0);

	srbuf src;
	sr_bufinit(&src);
	test_valloc(&src, &a, 2, 7);
	test_valloc(&src, &a, 3, 8);
	test_valloc(&src, &a, 4, 9);

	sriter stream;
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));
	ssmerge m;
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 0);
	t( ss_merge(&m) == 0 );
	t( m.b->n == 1 );
	t( m.result.n == 1 );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 2 );
	t( m.result.lsnmax == 4 );
	validate_index(&store, &c, &m.result);
	i = m.result;

	freebuf(&src, &a);
	sr_bufinit(&src);
	test_valloc(&src, &a, 5, 7);
	test_valloc(&src, &a, 6, 8);
	test_valloc(&src, &a, 7, 9);

	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 8);
	t( ss_merge(&m) == 0 );
	t( m.b->n == 1 );
	t( m.result.n == 1 );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 5 );
	t( m.result.lsnmax == 7 );
	validate_index(&store, &c, &m.result);
	ss_mergefree(&m);

	t( ss_close(&store, &c) == 0 );
	ss_indexfree(&i, &a);
	freebuf(&src, &a);
	ss_cfree(&sc, &c);
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_single_remerge1(void)
{
	rmrf("./test");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	srseq seq;
	sr_seqinit(&seq);
	t( sr_confinit(&conf, &a) == 0 );
	confset(&conf, &a, "env.dir", "test");
	confset(&conf, &a, "env.page_size", 4);
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);
	ssc sc;
	ss_cinit(&sc, &c);
	ss store;
	ss_init(&store);
	t( ss_open(&store, &c) == 1 );

	ssindex i;
	ss_indexinit(&i, 0);

	srbuf src;
	sr_bufinit(&src);
	test_valloc(&src, &a, 2, 7);
	test_valloc(&src, &a, 3, 8);
	test_valloc(&src, &a, 4, 9);

	sriter stream;
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));
	ssmerge m;
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 0);
	t( ss_merge(&m) == 0 );
	t( m.b->n == 1 );
	t( m.result.n == 1 );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 2 );
	t( m.result.lsnmax == 4 );
	validate_index(&store, &c, &m.result);
	i = m.result;

	freebuf(&src, &a);
	sr_bufinit(&src);
	test_valloc(&src, &a, 6, 8);
	test_valloc(&src, &a, 7, 9);

	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 8);
	t( ss_merge(&m) == 0 );
	t( m.b->n == 1 );
	t( m.result.n == 1 );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 2 );
	t( m.result.lsnmax == 7 );
	validate_index(&store, &c, &m.result);
	ss_mergefree(&m);

	t( ss_close(&store, &c) == 0 );
	ss_indexfree(&i, &a);
	freebuf(&src, &a);
	ss_cfree(&sc, &c);
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_single_remergegc(void)
{
	rmrf("./test");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	srseq seq;
	sr_seqinit(&seq);
	t( sr_confinit(&conf, &a) == 0 );
	confset(&conf, &a, "env.dir", "test");
	confset(&conf, &a, "env.page_size", 4);
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);
	ssc sc;
	ss_cinit(&sc, &c);
	ss store;
	ss_init(&store);
	t( ss_open(&store, &c) == 1 );

	ssindex i;
	ss_indexinit(&i, 0);

	srbuf src;
	sr_bufinit(&src);
	test_valloc(&src, &a, 2, 7);
	test_valloc(&src, &a, 3, 8);
	test_valloc(&src, &a, 4, 9);

	sriter stream;
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));
	ssmerge m;
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 0);
	t( ss_merge(&m) == 0 );
	t( m.b->n == 1 );
	t( m.result.n == 1 );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 2 );
	t( m.result.lsnmax == 4 );
	validate_index(&store, &c, &m.result);
	i = m.result;

	freebuf(&src, &a);
	sr_bufinit(&src);
	test_valloc(&src, &a, 5, 7);
	test_valloc(&src, &a, 6, 8);
	test_valloc(&src, &a, 7, 9);

	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 0);
	t( ss_merge(&m) == 0 );
	t( m.b->n == 1 );
	t( m.result.n == 1 );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 2 );
	t( m.result.lsnmax == 7 );
	validate_index(&store, &c, &m.result);
	ss_mergefree(&m);

	t( ss_close(&store, &c) == 0 );
	ss_indexfree(&i, &a);
	freebuf(&src, &a);
	ss_cfree(&sc, &c);
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_n(void)
{
	rmrf("./test");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	srseq seq;
	sr_seqinit(&seq);
	t( sr_confinit(&conf, &a) == 0 );
	confset(&conf, &a, "env.dir", "test");
	confset(&conf, &a, "env.page_size", 4);
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);
	ssc sc;
	ss_cinit(&sc, &c);
	ss store;
	ss_init(&store);
	t( ss_open(&store, &c) == 1 );

	ssindex i;
	ss_indexinit(&i, 0);

	srbuf src;
	sr_bufinit(&src);

	int k = 0;
	while (k < 141) {
		test_valloc(&src, &a, k, k);
		k++;
	}

	sriter stream;
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));

	ssmerge m;
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 0);
	t( ss_merge(&m) == 0 );
	int N = 141 / conf.page_size + 1;
	t( m.b->n == N  );
	t( m.result.n == N );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 0 );
	t( m.result.lsnmax == 140 );

	validate_index(&store, &c, &m.result);

	ss_mergefree(&m);

	t( ss_close(&store, &c) == 0 );
	ss_cfree(&sc, &c);
	freebuf(&src, &a);
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_n_remerge0(void)
{
	rmrf("./test");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	srseq seq;
	sr_seqinit(&seq);
	t( sr_confinit(&conf, &a) == 0 );
	confset(&conf, &a, "env.dir", "test");
	confset(&conf, &a, "env.page_size", 4);
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);
	ssc sc;
	ss_cinit(&sc, &c);
	ss store;
	ss_init(&store);
	t( ss_open(&store, &c) == 1 );

	ssindex i;
	ss_indexinit(&i, 0);
	srbuf src;
	sr_bufinit(&src);
	int k = 0;
	while (k < 141) {
		test_valloc(&src, &a, k, k);
		k++;
	}
	sriter stream;
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));
	ssmerge m;
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 0);
	t( ss_merge(&m) == 0 );
	int N = 141 / conf.page_size + 1;
	t( m.b->n == N  );
	t( m.result.n == N );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 0 );
	t( m.result.lsnmax == 140 );
	validate_index(&store, &c, &m.result);
	i = m.result;

	freebuf(&src, &a);
	sr_bufinit(&src);
	k = 0;
	while (k < 141) {
		test_valloc(&src, &a, 141 + k, k);
		k++;
	}
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 500);
	t( ss_merge(&m) == 0 );
	t( m.b->n == N  );
	t( m.result.n == N );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 141 );
	t( m.result.lsnmax == 141 + 141 - 1);
	validate_index(&store, &c, &m.result);
	ss_mergefree(&m);

	t( ss_close(&store, &c) == 0 );

	ss_indexfree(&i, &a);
	freebuf(&src, &a);
	ss_cfree(&sc, &c);
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_n_remerge1(void)
{
	rmrf("./test");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	srseq seq;
	sr_seqinit(&seq);
	t( sr_confinit(&conf, &a) == 0 );
	confset(&conf, &a, "env.dir", "test");
	confset(&conf, &a, "env.page_size", 4);
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);
	ssc sc;
	ss_cinit(&sc, &c);
	ss store;
	ss_init(&store);
	t( ss_open(&store, &c) == 1 );

	ssindex i;
	ss_indexinit(&i, 0);
	srbuf src;
	sr_bufinit(&src);
	int k = 0;
	while (k < 36) {
		test_valloc(&src, &a, k, k);
		k++;
	}
	sriter stream;
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));
	ssmerge m;
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 0);
	t( ss_merge(&m) == 0 );
	int N = 36 / conf.page_size;
	t( m.b->n == N  );
	t( m.result.n == N );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 0 );
	t( m.result.lsnmax == 35 );
	validate_index(&store, &c, &m.result);
	i = m.result;

	freebuf(&src, &a);
	sr_bufinit(&src);
	k = 0;
	while (k < 36) {
		test_valloc(&src, &a, 36 + k, k);
		k++;
	}
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 500);
	t( ss_merge(&m) == 0 );
	t( m.b->n == N  );
	t( m.result.n == N );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 36 );
	t( m.result.lsnmax == 36 + 36 - 1);
	validate_index(&store, &c, &m.result);
	ss_mergefree(&m);

	t( ss_close(&store, &c) == 0 );

	ss_indexfree(&i, &a);
	freebuf(&src, &a);
	ss_cfree(&sc, &c);
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_n_remerge2(void)
{
	rmrf("./test");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	srseq seq;
	sr_seqinit(&seq);
	t( sr_confinit(&conf, &a) == 0 );
	confset(&conf, &a, "env.dir", "test");
	confset(&conf, &a, "env.page_size", 4);
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);
	ssc sc;
	ss_cinit(&sc, &c);
	ss store;
	ss_init(&store);
	t( ss_open(&store, &c) == 1 );

	ssindex i;
	ss_indexinit(&i, 0);
	srbuf src;
	sr_bufinit(&src);
	int k = 0;
	while (k < 36) {
		test_valloc(&src, &a, k, k);
		k++;
	}
	sriter stream;
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));
	ssmerge m;
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 0);
	t( ss_merge(&m) == 0 );
	int N = 36 / conf.page_size;
	t( m.b->n == N  );
	t( m.result.n == N );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 0 );
	t( m.result.lsnmax == 35 );
	validate_index(&store, &c, &m.result);
	i = m.result;

	freebuf(&src, &a);
	sr_bufinit(&src);
	k = 0;
	while (k < 36) {
		test_valloc(&src, &a, 36 + k, k);
		k++;
	}
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 500);
	t( ss_merge(&m) == 0 );
	t( m.b->n == N  );
	t( m.result.n == N );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 36 );
	t( m.result.lsnmax == 36 + 36 - 1);
	validate_index(&store, &c, &m.result);
	ss_indexfree(&i, &a);
	i = m.result;

	freebuf(&src, &a);
	sr_bufinit(&src);
	k = 0;
	while (k < 36) {
		test_valloc(&src, &a, 36 + 36 + k, k);
		k++;
	}
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 500);
	t( ss_merge(&m) == 0 );
	t( m.b->n == N  );
	t( m.result.n == N );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 36 + 36 );
	t( m.result.lsnmax == 36 + 36 + 36 - 1);
	validate_index(&store, &c, &m.result);
	ss_mergefree(&m);

	t( ss_close(&store, &c) == 0 );

	ss_indexfree(&i, &a);
	freebuf(&src, &a);
	ss_cfree(&sc, &c);
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_n_remerge3(void)
{
	rmrf("./test");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	srseq seq;
	sr_seqinit(&seq);
	t( sr_confinit(&conf, &a) == 0 );
	confset(&conf, &a, "env.dir", "test");
	confset(&conf, &a, "env.page_size", 4);
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);
	ssc sc;
	ss_cinit(&sc, &c);
	ss store;
	ss_init(&store);
	t( ss_open(&store, &c) == 1 );

	ssindex i;
	ss_indexinit(&i, 0);
	srbuf src;
	sr_bufinit(&src);

	int rounds = 0;
	int N = 412 / conf.page_size;
	int lsn = 0;

	while (rounds < 10)
	{
		freebuf(&src, &a);
		sr_bufinit(&src);
		int k = 0;
		while (k < 412) {
			test_valloc(&src, &a, lsn, k);
			k++;
			lsn++;
		}
		sriter stream;
		sr_iterinit(&stream, &sr_bufiter, &c);
		sr_iteropen(&stream, &src, sizeof(sv));
		ssmerge m;
		ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 10000);
		t( ss_merge(&m) == 0 );
		t( m.b->n == N);
		t( m.result.n == N);
		t( ss_mergewrite(&m, NULL, NULL) == 0 );
		t( m.result.lsnmin ==  (rounds * 412));
		t( m.result.lsnmax == ((rounds + 1) * 412) - 1);
		validate_index(&store, &c, &m.result);
		ss_indexfree(&i, &a);
		i = m.result;

		rounds++;
	}
	t( ss_close(&store, &c) == 0 );

	ss_indexfree(&i, &a);
	freebuf(&src, &a);
	ss_cfree(&sc, &c);
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_n_remerge4(void)
{
	rmrf("./test");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	srseq seq;
	sr_seqinit(&seq);
	t( sr_confinit(&conf, &a) == 0 );
	confset(&conf, &a, "env.dir", "test");
	confset(&conf, &a, "env.page_size", 4);
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);
	ssc sc;
	ss_cinit(&sc, &c);
	ss store;
	ss_init(&store);
	t( ss_open(&store, &c) == 1 );

	ssindex i;
	ss_indexinit(&i, 0);
	srbuf src;
	sr_bufinit(&src);

	int rounds = 0;
	int N = 412 / conf.page_size;
	int lsn = 0;

	while (rounds < 10)
	{
		freebuf(&src, &a);
		sr_bufinit(&src);
		int k = 0;
		while (k < 412) {
			test_valloc(&src, &a, lsn, k);
			k++;
			lsn++;
		}
		sriter stream;
		sr_iterinit(&stream, &sr_bufiter, &c);
		sr_iteropen(&stream, &src, sizeof(sv));
		ssmerge m;
		ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 0);
		t( ss_merge(&m) == 0 );
		t( m.b->n == N);
		t( m.result.n == N);
		t( ss_mergewrite(&m, NULL, NULL) == 0 );
		t( m.result.lsnmin == 0);
		t( m.result.lsnmax == ((rounds + 1) * 412) - 1);
		validate_index(&store, &c, &m.result);
		ss_indexfree(&i, &a);
		i = m.result;

		rounds++;
	}
	t( ss_close(&store, &c) == 0 );

	ss_indexfree(&i, &a);
	freebuf(&src, &a);
	ss_cfree(&sc, &c);
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_n_seq(void)
{
	rmrf("./test");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	srseq seq;
	sr_seqinit(&seq);
	t( sr_confinit(&conf, &a) == 0 );
	confset(&conf, &a, "env.dir", "test");
	confset(&conf, &a, "env.page_size", 128);
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);
	ssc sc;
	ss_cinit(&sc, &c);
	ss store;
	ss_init(&store);
	t( ss_open(&store, &c) == 1 );

	ssindex i;
	ss_indexinit(&i, 0);

	srbuf src;
	sr_bufinit(&src);

	int count = 4096;
	int k = 0;
	while (k < count) {
		test_valloc(&src, &a, k, k);
		k++;
	}

	sriter stream;
	sr_iterinit(&stream, &sr_bufiter, &c);
	sr_iteropen(&stream, &src, sizeof(sv));

	ssmerge m;
	ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 0);
	t( ss_merge(&m) == 0 );
	int N = count / conf.page_size;
	t( m.b->n == N  );
	t( m.result.n == N );
	t( ss_mergewrite(&m, NULL, NULL) == 0 );
	t( m.result.lsnmin == 0 );
	t( m.result.lsnmax == count - 1 );
	validate_index(&store, &c, &m.result);

	ss_mergefree(&m);

	t( ss_close(&store, &c) == 0 );

	freebuf(&src, &a);
	ss_cfree(&sc, &c);
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}
static void
test_merge_n_seq_remerge(void)
{
	rmrf("./test");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	srseq seq;
	sr_seqinit(&seq);
	t( sr_confinit(&conf, &a) == 0 );
	confset(&conf, &a, "env.dir", "test");
	confset(&conf, &a, "env.page_size", 128);
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);
	ssc sc;
	ss_cinit(&sc, &c);
	ss store;
	ss_init(&store);
	t( ss_open(&store, &c) == 1 );

	ssindex i;
	ss_indexinit(&i, 0);

	srbuf src;
	sr_bufinit(&src);

	int count = 974;
	int rounds = 0;
	int N = count / conf.page_size + 1;
	int lsn = 0;

	while (rounds < 10)
	{
		freebuf(&src, &a);
		sr_bufinit(&src);
		int k = 0;
		while (k < count) {
			test_valloc(&src, &a, lsn, k);
			k++;
			lsn++;
		}
		sriter stream;
		sr_iterinit(&stream, &sr_bufiter, &c);
		sr_iteropen(&stream, &src, sizeof(sv));
		ssmerge m;
		ss_mergeinit(&m, &store, &c, &sc, 0, &i, &stream, 4, 0);
		t( ss_merge(&m) == 0 );
		t( m.b->n == N );
		t( m.result.n == N );
		t( ss_mergewrite(&m, NULL, NULL) == 0 );
		t( m.result.lsnmin == 0 );
		t( m.result.lsnmax == ((rounds + 1) * count) - 1 );
		validate_index(&store, &c, &m.result);
		ss_indexfree(&i, &a);
		i = m.result;
		rounds++;
	}

	t( ss_close(&store, &c) == 0 );

	ss_indexfree(&i, &a);
	freebuf(&src, &a);
	ss_cfree(&sc, &c);
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

int
main(int argc, char *argv[])
{
	test( test_merge_empty );

	test( test_merge_single );
	test( test_merge_single_remerge0 );
	test( test_merge_single_remerge1 );
	test( test_merge_single_remergegc );
	test( test_merge_n );
	test( test_merge_n_remerge0 );
	test( test_merge_n_remerge1 );
	test( test_merge_n_remerge2 );
	test( test_merge_n_remerge3 );
	test( test_merge_n_remerge4 );
	test( test_merge_n_seq );
	test( test_merge_n_seq_remerge );
	return 0;
}
