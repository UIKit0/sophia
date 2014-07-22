
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

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>
#include <sophia.h>
#include "test.h"

static smv*
allocv(sra *a, uint64_t lsn, uint8_t flags, int *key)
{
	svlocal l;
	l.lsn         = lsn;
	l.flags       = flags;
	l.key         = key;
	l.keysize     = sizeof(int);
	l.value       = NULL;
	l.valuesize   = 0;
	l.valueoffset = 0;
	sv lv;
	svinit(&lv, &sv_localif, &l, NULL);
	return sm_valloc(a, &lv);
}

static inline smv*
getv(smindex *i, uint64_t lsvn, int *key) {
	return sm_indexmatch(i, lsvn, key, sizeof(int));
}

static void
test_vupdate(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	int key = 7;
	smv *h = allocv(&a, 0, SVSET, &key);
	smv *n = allocv(&a, 1, SVSET, &key);
	t( sm_vupdate(&a, h, n, 1) == n );
	t( n->next == NULL );
	sm_vfree(&a, n);

	h = allocv(&a, 0, SVSET, &key);
	n = allocv(&a, 1, SVSET, &key);
	t( sm_vupdate(&a, h, n, 0) == n );
	t( n->next == h );
	t( h->next == NULL );
	sm_vfree(&a, n);

	h = allocv(&a, 0, SVSET, &key);
	n = allocv(&a, 1, SVSET, &key);
	smv *m = allocv(&a, 2, SVSET, &key);
	t( sm_vupdate(&a, h, n, 0) == n );
	t( sm_vupdate(&a, n, m, 0) == m );
	t( m->next == n );
	t( n->next == h );
	t( h->next == NULL );
	sm_vfree(&a, m);

	h = allocv(&a, 0, SVSET, &key);
	n = allocv(&a, 1, SVSET, &key);
	m = allocv(&a, 2, SVSET, &key);
	t( sm_vupdate(&a, h, n, 1) == n );
	t( sm_vupdate(&a, n, m, 1) == m );
	t( m->next == n );
	t( n->next == NULL );
	sm_vfree(&a, m);

	h = allocv(&a, 0, SVSET, &key);
	n = allocv(&a, 1, SVSET, &key);
	t( sm_vupdate(&a, h, n, 1) == n );
	h = n;
	n = allocv(&a, 2, SVSET, &key);
	t( sm_vupdate(&a, h, n, 2) == n );
	h = n;
	n = allocv(&a, 3, SVSET, &key);
	t( sm_vupdate(&a, h, n, 3) == n );
	t( n->next == NULL );
	sm_vfree(&a, n);

	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_replace0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	smindex i;
	t( sm_indexinit(&i, &c) == 0 );

	int key = 7;
	smv *h = allocv(&a, 0, SVSET, &key);
	smv *n = allocv(&a, 1, SVSET, &key);
	t( sm_indexreplace(&i, &a, 0, h) == 0 );
	t( sm_indexreplace(&i, &a, 1, n) == 0 );
	smv *p = getv(&i, 1, &key);
	t( p == n );
	t( n->next == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_replace1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	smindex i;
	t( sm_indexinit(&i, &c) == 0 );

	int key = 7;
	smv *h = allocv(&a, 0, SVSET, &key);
	smv *n = allocv(&a, 1, SVSET, &key);
	t( sm_indexreplace(&i, &a, 0, h) == 0 );
	t( sm_indexreplace(&i, &a, 0, n) == 0 );
	smv *p = getv(&i, 1, &key);
	t( p == n );
	t( n->next == h );
	t( h->next == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_replace2(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	smindex i;
	t( sm_indexinit(&i, &c) == 0 );

	int key = 7;
	smv *h = allocv(&a, 0, SVSET, &key);
	smv *n = allocv(&a, 1, SVSET, &key);
	smv *p = allocv(&a, 2, SVSET, &key);
	t( sm_indexreplace(&i, &a, 0, h) == 0 );
	t( sm_indexreplace(&i, &a, 0, n) == 0 );
	t( sm_indexreplace(&i, &a, 0, p) == 0 );

	smv *q = getv(&i, 1, &key);
	t( q == n );
	t( q->next == h );
	t( h->next == NULL );

	q = getv(&i, 0, &key);
	t( q == h );
	t( h->next == NULL );

	q = getv(&i, 2, &key);
	t( q == p );
	t( p->next == n );
	t( n->next == h );
	t( h->next == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

int
main(int argc, char *argv[])
{
	test( test_vupdate );
	test( test_replace0 );
	test( test_replace1 );
	test( test_replace2 );
	return 0;
}
