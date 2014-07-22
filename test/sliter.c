
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
#include <sophia.h>
#include "test.h"

static void
alloclogv(svlog *log, sra *a, uint64_t lsn, uint8_t flags, int key)
{
	svlocal l;
	l.lsn         = lsn;
	l.flags       = flags;
	l.key         = &key;
	l.keysize     = sizeof(int);
	l.value       = NULL;
	l.valuesize   = 0;
	l.valueoffset = 0;
	sv lv;
	svinit(&lv, &sv_localif, &l, NULL);
	smv *v = sm_valloc(a, &lv);
	t( v != NULL );
	sv vv;
	svinit(&vv, &sm_vif, v, NULL);
	t( sv_logadd(log, a, &vv) == 0 );
}

static void
freelog(svlog *log, src *c)
{
	sriter i;
	sr_iterinit(&i, &sr_bufiter, c);
	sr_iteropen(&i, &log->buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sv *v = sr_iterof(&i);
		sr_free(c->a, v->v);
	}
	sv_logfree(log, c->a);
}

static void
test_tx(void)
{
	rmrf("./log");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srseq seq;
	sr_seqinit(&seq);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	t( sr_confinit(&conf, &a) == 0 );
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);

	slpool lp;
	t( sl_poolinit(&lp, &c) == 0 );
	t( sl_poolopen(&lp) == 0 );
	t( sl_poolrotate(&lp) == 0 );

	svlog log;
	sv_loginit(&log);

	alloclogv(&log, &a, 0, SVSET, 7);

	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log, 0) == 0 );
	t( sl_commit(&ltx) == 0 );

	freelog(&log, &c);
	t( sl_poolshutdown(&lp) == 0 );

	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_tx_read_empty(void)
{
	rmrf("./log");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srseq seq;
	sr_seqinit(&seq);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	t( sr_confinit(&conf, &a) == 0 );
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);

	slpool lp;
	t( sl_poolinit(&lp, &c) == 0 );
	t( sl_poolopen(&lp) == 0 );
	t( sl_poolrotate(&lp) == 0 );
	svlog log;
	sv_loginit(&log);
	freelog(&log, &c);

	sl *current = srcast(lp.list.prev, sl, link);
	sriter li;
	sr_iterinit(&li, &sl_iter, &c);
	t( sr_iteropen(&li, &current->file, 1) == 0 );
	for (;;) {
		// begin
		while (sr_iterhas(&li)) {
			sv *v = sr_iterof(&li);
			t( *(int*)svkey(v) == 7 );
			sr_iternext(&li);
		}
		t( sl_itererror(&li) == 0 );
		// commit
		if (! sl_itercontinue(&li) )
			break;
	}
	sr_iterclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_tx_read0(void)
{
	rmrf("./log");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srseq seq;
	sr_seqinit(&seq);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	t( sr_confinit(&conf, &a) == 0 );
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);

	slpool lp;
	t( sl_poolinit(&lp, &c) == 0 );
	t( sl_poolopen(&lp) == 0 );
	t( sl_poolrotate(&lp) == 0 );
	svlog log;
	sv_loginit(&log);
	alloclogv(&log, &a, 0, SVSET, 7);
	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log, 0) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &c);

	sl *current = srcast(lp.list.prev, sl, link);
	sriter li;
	sr_iterinit(&li, &sl_iter, &c);
	t( sr_iteropen(&li, &current->file, 1) == 0 );
	for (;;) {
		// begin
		while (sr_iterhas(&li)) {
			sv *v = sr_iterof(&li);
			t( *(int*)svkey(v) == 7 );
			sr_iternext(&li);
		}
		t( sl_itererror(&li) == 0 );
		// commit
		if (! sl_itercontinue(&li) )
			break;
	}
	sr_iterclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_tx_read1(void)
{
	rmrf("./log");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srseq seq;
	sr_seqinit(&seq);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	t( sr_confinit(&conf, &a) == 0 );
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);

	slpool lp;
	t( sl_poolinit(&lp, &c) == 0 );
	t( sl_poolopen(&lp) == 0 );
	t( sl_poolrotate(&lp) == 0 );
	svlog log;
	sv_loginit(&log);
	alloclogv(&log, &a, 0, SVSET, 7);
	alloclogv(&log, &a, 0, SVSET, 8);
	alloclogv(&log, &a, 0, SVSET, 9);
	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log, 0) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &c);

	sl *current = srcast(lp.list.prev, sl, link);
	sriter li;
	sr_iterinit(&li, &sl_iter, &c);
	t( sr_iteropen(&li, &current->file, 1) == 0 );
	for (;;) {
		// begin
		t( sr_iterhas(&li) == 1 );
		sv *v = sr_iterof(&li);
		t( *(int*)svkey(v) == 7 );
		sr_iternext(&li);
		t( sr_iterhas(&li) == 1 );
		v = sr_iterof(&li);
		t( *(int*)svkey(v) == 8 );
		sr_iternext(&li);
		t( sr_iterhas(&li) == 1 );
		v = sr_iterof(&li);
		t( *(int*)svkey(v) == 9 );
		sr_iternext(&li);
		t( sr_iterhas(&li) == 0 );

		t( sl_itererror(&li) == 0 );
		// commit
		if (! sl_itercontinue(&li) )
			break;
	}
	sr_iterclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_tx_read2(void)
{
	rmrf("./log");

	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srseq seq;
	sr_seqinit(&seq);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	srconf conf;
	t( sr_confinit(&conf, &a) == 0 );
	src c;
	sr_cinit(&c, &conf, &sdb, &seq, &a);

	slpool lp;
	t( sl_poolinit(&lp, &c) == 0 );
	t( sl_poolopen(&lp) == 0 );
	t( sl_poolrotate(&lp) == 0 );
	svlog log;
	sv_loginit(&log);
	alloclogv(&log, &a, 0, SVSET, 7);
	alloclogv(&log, &a, 0, SVSET, 8);
	alloclogv(&log, &a, 0, SVSET, 9);
	sltx ltx;
	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log, 0) == 0 );
	t( sl_commit(&ltx) == 0 );

	t( sl_begin(&lp, &ltx) == 0 );
	t( sl_write(&ltx, &log, 0) == 0 );
	t( sl_commit(&ltx) == 0 );
	freelog(&log, &c);

	sl *current = srcast(lp.list.prev, sl, link);
	sriter li;
	sr_iterinit(&li, &sl_iter, &c);
	t( sr_iteropen(&li, &current->file, 1) == 0 );
	for (;;) {
		// begin
		t( sr_iterhas(&li) == 1 );
		sv *v = sr_iterof(&li);
		t( *(int*)svkey(v) == 7 );
		sr_iternext(&li);
		t( sr_iterhas(&li) == 1 );
		v = sr_iterof(&li);
		t( *(int*)svkey(v) == 8 );
		sr_iternext(&li);
		t( sr_iterhas(&li) == 1 );
		v = sr_iterof(&li);
		t( *(int*)svkey(v) == 9 );
		sr_iternext(&li);

		t( sr_iterhas(&li) == 0 );
		t( sl_itererror(&li) == 0 );
		// commit
		if (! sl_itercontinue(&li) )
			break;
	}
	sr_iterclose(&li);

	t( sl_poolshutdown(&lp) == 0 );
	sr_schemefree(&sdb, &a);
	sr_conffree(&conf, &a);
	sr_cmpindex_free(&ci, &a);
}

int
main(int argc, char *argv[])
{
	test( test_tx );
	test( test_tx_read_empty );
	test( test_tx_read0 );
	test( test_tx_read1 );
	test( test_tx_read2 );
	return 0;
}
