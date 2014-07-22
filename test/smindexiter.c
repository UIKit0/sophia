
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
test_lte_empty(void)
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
	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_eq0(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 0, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 0, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 0, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &keyb, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &keyc, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_eq1(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 2, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 4, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 1);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &keyb, sizeof(int), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_minmax(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 4, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 3);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 1);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_mid0(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 4, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	int key = 1;
	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	key = 3;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	key = 6;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	key = 8;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_mid1(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 4, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	int key = 15;
	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 1);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_iterate0(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 4, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	int key = 15;
	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == va );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_iterate1(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 4, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	int key = 15;
	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(key), 3);

	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_iterate2(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 4, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, NULL, 0, 2);

	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lt_eq(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 0, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 0, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 0, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LT, &keya, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LT, &keyb, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LT, &keyc, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lt_iterate(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 4, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LT, &keya, sizeof(keya), 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_dup_eq(void)
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

	int keya = 7;

	smv *va = allocv(&a, 1, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 2, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 3, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_dup_mid(void)
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

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	smv *p = allocv(&a, 0, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, p) == 0 );
	p = allocv(&a, 4, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, p) == 0 );

	smv *va = allocv(&a, 1, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 2, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 3, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &keya, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &keyc, sizeof(int), 5);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == p );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_dup_iterate(void)
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

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	smv *h = allocv(&a, 0, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, h) == 0 );
	smv *p = allocv(&a, 2, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, p) == 0 );

	smv *va = allocv(&a, 1, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 2, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 3, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	int key = 20;
	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_LTE, &key, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == p );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == h );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_empty(void)
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
	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_eq0(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 0, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 0, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 0, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &keyb, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &keyc, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vc );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_eq1(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 2, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 4, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 1);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &keyb, sizeof(int), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_minmax(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 2, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 4, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, NULL, 0, 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, NULL, 0, 3);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, NULL, 0, 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, NULL, 0, 1);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_mid0(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 4, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	int key = 1;
	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	key = 3;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	key = 6;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	key = 8;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_mid1(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 2, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 4, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	int key = 1;
	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 1);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_iterate0(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 4, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	int key = 0;
	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_iterate1(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 2, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 4, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	int key = 1;
	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(key), 3);

	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gt_eq(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 0, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 0, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 0, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GT, &keya, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GT, &keyb, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GT, &keyc, sizeof(int), 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gt_iterate(void)
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

	int keya = 7;
	int keyb = 5;
	int keyc = 2;
	smv *va = allocv(&a, 4, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 3, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 2, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GT, &keyc, sizeof(keya), 8);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_dup_eq(void)
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

	int keya = 7;

	smv *va = allocv(&a, 1, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 2, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 3, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_dup_mid(void)
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

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	smv *p = allocv(&a, 0, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, p) == 0 );
	p = allocv(&a, 4, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, p) == 0 );

	smv *va = allocv(&a, 1, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 2, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 3, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == vc );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == vb );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &keya, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == va );

	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &keyc, sizeof(int), 5);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == p );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_dup_iterate(void)
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

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	smv *h = allocv(&a, 0, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, h) == 0 );
	smv *p = allocv(&a, 2, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, p) == 0 );

	smv *va = allocv(&a, 1, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 2, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 3, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	int key = 2;
	sriter it;
	sr_iterinit(&it, &sm_indexiter, &c);
	sr_iteropen(&it, &i, SR_GTE, &key, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == h );
	sr_iternext(&it);

	v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( v->v == p );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_iterate_raw(void)
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

	int keyb = 3;
	int keya = 7;
	int keyc = 15;

	smv *h = allocv(&a, 0, SVSET, &keyb);
	t( sm_indexreplace(&i, &a, 0, h) == 0 );
	smv *p = allocv(&a, 2, SVSET, &keyc);
	t( sm_indexreplace(&i, &a, 0, p) == 0 );

	smv *va = allocv(&a, 1, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, va) == 0 );
	smv *vb = allocv(&a, 2, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vb) == 0 );
	smv *vc = allocv(&a, 3, SVSET, &keya);
	t( sm_indexreplace(&i, &a, 0, vc) == 0 );

	sriter it;
	sr_iterinit(&it, &sm_indexiterraw, &c);
	sr_iteropen(&it, &i);

	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v->v == h );
	sr_iternext(&it);

	v = sr_iterof(&it);
	t( v->v == vc );
	sr_iternext(&it);

	v = sr_iterof(&it);
	t( v->v == vb );
	sr_iternext(&it);

	v = sr_iterof(&it);
	t( v->v == va );
	sr_iternext(&it);

	v = sr_iterof(&it);
	t( v->v == p );
	sr_iternext(&it);

	v = sr_iterof(&it);
	t( v == NULL );

	sm_indexfree(&i);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

int
main(int argc, char *argv[])
{
	test( test_lte_empty );
	test( test_lte_eq0 );
	test( test_lte_eq1 );
	test( test_lte_minmax );
	test( test_lte_mid0 );
	test( test_lte_mid1 );
	test( test_lte_iterate0 );
	test( test_lte_iterate1 );
	test( test_lte_iterate2 );
	test( test_lt_eq );
	test( test_lt_iterate );
	test( test_lte_dup_eq );
	test( test_lte_dup_mid );
	test( test_lte_dup_iterate );

	test( test_gte_empty );
	test( test_gte_eq0 );
	test( test_gte_eq1 );
	test( test_gte_minmax );
	test( test_gte_mid0 );
	test( test_gte_mid1 );
	test( test_gte_iterate0 );
	test( test_gte_iterate1 );
	test( test_gt_eq );
	test( test_gt_iterate );
	test( test_gte_dup_eq );
	test( test_gte_dup_mid );
	test( test_gte_dup_iterate );

	test( test_iterate_raw );
	return 0;
}
