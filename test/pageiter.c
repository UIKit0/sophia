
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
#include <libsl.h>
#include <libss.h>
#include <sophia.h>
#include "test.h"

static void
addv(sspagebuild *b, uint64_t lsn, uint8_t flags, int *key)
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
	t( ss_pagebuild_add(b, &lv) == 0 );
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &i, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &j, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &k, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &i, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &j, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &k, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &i, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &j, sizeof(int), 1);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &k, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_eq2(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &i, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &j, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &k, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_minmax0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int min = 6;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &min, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	t( sr_iterof(&it) == NULL);

	int max = 16;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &max, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_minmax1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int z = 2;
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 4, SVSET, &z);
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int min = 6;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &min, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == z);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &min, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	int max = 16;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &max, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &max, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &max, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &max, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &max, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_minmax2(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int z = 2;
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 1, SVSET, &z);
	addv(&b, 2, SVSET, &i);
	addv(&b, 3, SVSET, &j);
	addv(&b, 4, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int max = 16;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &max, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &max, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &max, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &max, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == z);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &max, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL);

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 8;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	p = 10;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);

	p = 555;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 8;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 2);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	p = 10;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 1);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	p = 555;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, NULL, 0, 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &k, sizeof(k), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LT, &i, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LT, &j, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LT, &k, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lt_minmax(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int min = 7;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LT, &min, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	t( sr_iterof(&it) == NULL);

	int max = 16;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &max, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lt_mid(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 8;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LT, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	p = 10;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LT, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);

	p = 555;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LT, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lt_iterate0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LT, NULL, 0, 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lt_iterate1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);
	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);
	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LT, &k, sizeof(k), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);
	int j = 4;
	int i = 7;
	addv(&b, 0, SVSET, &j);
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET|SVDUP, &i);
	addv(&b, 1, SVSET|SVDUP, &i);
	ss_pagebuild_end(&b);
	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, NULL, 0, 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	t( svlsn(v) == 3 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, NULL, 0, 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	t( svlsn(v) == 2 ); sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, NULL, 0, 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	t( svlsn(v) == 1 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, NULL, 0, 0);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	t( svlsn(v) == 0 );

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 5, SVSET, &i);
	addv(&b, 4, SVSET, &j);
	addv(&b, 3, SVSET|SVDUP, &j);
	addv(&b, 2, SVSET|SVDUP, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 8;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, NULL, 0, 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	p = 10;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j );
	t( svlsn(v) == 2 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j );
	t( svlsn(v) == 3 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j );
	t( svlsn(v) == 4 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 10);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j );
	t( svlsn(v) == 4 );

	p = 8;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i );
	t( svlsn(v) == 5 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, NULL, 0, 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_dup_mid_gt(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);
	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 90, SVSET, &i);
	addv(&b, 80, SVSET, &j);
	addv(&b, 70, SVSET|SVDUP, &j);
	addv(&b, 60, SVSET|SVDUP, &j);
	addv(&b, 50, SVSET, &k);
	addv(&b, 40, SVSET|SVDUP, &k);
	addv(&b, 30, SVSET|SVDUP, &k);
	ss_pagebuild_end(&b);
	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 16;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, NULL, 0, 4);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 30);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 30);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 38);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 30);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 40);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 40);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 50);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 50);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &j, sizeof(int), 90);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	t( svlsn(v) == 80);

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_dup_mid_lt(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);
	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 3;
	addv(&b, 50, SVSET, &k);
	addv(&b, 40, SVSET|SVDUP, &k);
	addv(&b, 30, SVSET|SVDUP, &k);
	addv(&b, 90, SVSET, &i);
	addv(&b, 80, SVSET, &j);
	addv(&b, 70, SVSET|SVDUP, &j);
	addv(&b, 60, SVSET|SVDUP, &j);
	ss_pagebuild_end(&b);
	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 6;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 30);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 30);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 38);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 30);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 40);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 40);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 50);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 50);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &j, sizeof(int), 90);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	t( svlsn(v) == 80);

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_dup_iterate0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);
	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 90, SVSET, &i);
	addv(&b, 80, SVSET, &j);
	addv(&b, 70, SVSET|SVDUP, &j);
	addv(&b, 60, SVSET|SVDUP, &j);
	addv(&b, 50, SVSET, &k);
	addv(&b, 40, SVSET|SVDUP, &k);
	addv(&b, 30, SVSET|SVDUP, &k);
	ss_pagebuild_end(&b);
	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 100;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 100);

	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 50);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	t( svlsn(v) == 80);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	t( svlsn(v) == 90);
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lte_dup_iterate1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);
	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 42, SVSET, &i);
	addv(&b, 80, SVSET, &j);
	addv(&b, 60, SVSET|SVDUP, &j);
	addv(&b, 41, SVSET|SVDUP, &j);
	addv(&b, 50, SVSET, &k);
	addv(&b, 40, SVSET|SVDUP, &k);
	addv(&b, 30, SVSET|SVDUP, &k);
	ss_pagebuild_end(&b);
	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 100;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 30);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 30);
	sr_iternext(&it);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, &p, sizeof(int), 42);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 40);
	sr_iternext(&it);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	t( svlsn(v) == 41);
	sr_iternext(&it);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	t( svlsn(v) == 42);
	sr_iternext(&it);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_LTE, NULL, 0, 42);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 40);
	sr_iternext(&it);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	t( svlsn(v) == 41);
	sr_iternext(&it);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	t( svlsn(v) == 42);
	sr_iternext(&it);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &i, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &j, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &k, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &i, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &j, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &k, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_eq2(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &i, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &j, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &k, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_minmax0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int min = 6;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &min, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	int max = 16;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &max, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_minmax1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int z = 2;
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 4, SVSET, &z);
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int min = 6;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &min, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &min, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );


	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &min, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &min, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &min, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	int max = 16;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &max, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &max, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_minmax2(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int z = 2;
	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 4, SVSET, &z);
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int max = 2;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &max, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == z);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &max, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &max, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &max, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &max, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL);

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 8;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);

	p = 10;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	p = 2;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	p = 555;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 8;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	p = 10;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	p = 1;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, NULL, 0, 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &i, sizeof(k), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GT, &i, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( v != NULL);
	t( *(int*)svkey(v) == j);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GT, &j, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GT, &k, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gt_minmax(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 8;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int min = 7;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GT, &min, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);

	int max = 15;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GT, &max, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gt_mid(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 8;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GT, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);

	p = 10;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GT, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	p = 555;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GT, &p, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gt_iterate0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GT, NULL, 0, 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gt_iterate1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);
	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);
	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GT, &i, sizeof(i), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);
	int j = 4;
	int i = 7;
	addv(&b, 4, SVSET, &j);
	addv(&b, 3, SVSET, &i);
	addv(&b, 2, SVSET|SVDUP, &i);
	addv(&b, 1, SVSET|SVDUP, &i);
	ss_pagebuild_end(&b);
	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, NULL, 0, 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	t( svlsn(v) == 4 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, NULL, 0, 3);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	t( svlsn(v) == 3 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, NULL, 0, 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	t( svlsn(v) == 2 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, NULL, 0, 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	t( svlsn(v) == 1 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, NULL, 0, 0);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 5, SVSET, &i);
	addv(&b, 4, SVSET, &j);
	addv(&b, 3, SVSET|SVDUP, &j);
	addv(&b, 2, SVSET|SVDUP, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 8;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	t( svlsn(v) == 4 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, NULL, 0, 1);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);

	p = 10;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k );
	t( svlsn(v) == 1 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k );
	t( svlsn(v) == 1 );

	p = 8;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 8);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j );
	t( svlsn(v) == 4 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 3);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j );
	t( svlsn(v) == 3 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 2);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j );
	t( svlsn(v) == 2 );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, NULL, 0, 6);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_dup_mid_gt(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);
	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 90, SVSET, &i);
	addv(&b, 80, SVSET, &j);
	addv(&b, 70, SVSET|SVDUP, &j);
	addv(&b, 60, SVSET|SVDUP, &j);
	addv(&b, 50, SVSET, &k);
	addv(&b, 40, SVSET|SVDUP, &k);
	addv(&b, 30, SVSET|SVDUP, &k);
	ss_pagebuild_end(&b);
	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 6;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 4);
	t( sr_iterhas(&it) == 0 );
	sv *v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 30);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 30);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 38);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 30);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 40);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 40);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 50);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 50);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &j, sizeof(int), 90);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	t( svlsn(v) == 80);

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_dup_mid_lt(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);
	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int k = 7;
	int i = 8;
	int j = 9;
	addv(&b, 50, SVSET, &k);
	addv(&b, 40, SVSET|SVDUP, &k);
	addv(&b, 30, SVSET|SVDUP, &k);
	addv(&b, 90, SVSET, &i);
	addv(&b, 80, SVSET, &j);
	addv(&b, 70, SVSET|SVDUP, &j);
	addv(&b, 60, SVSET|SVDUP, &j);
	ss_pagebuild_end(&b);
	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 6;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 30);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 30);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 38);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 30);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 40);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 40);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 50);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 50);

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &j, sizeof(int), 90);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	t( svlsn(v) == 80);

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_dup_iterate0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);
	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 90, SVSET, &i);
	addv(&b, 80, SVSET, &j);
	addv(&b, 70, SVSET|SVDUP, &j);
	addv(&b, 60, SVSET|SVDUP, &j);
	addv(&b, 50, SVSET, &k);
	addv(&b, 40, SVSET|SVDUP, &k);
	addv(&b, 30, SVSET|SVDUP, &k);
	ss_pagebuild_end(&b);
	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 1;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 100);

	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	t( svlsn(v) == 90);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	t( svlsn(v) == 80);
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 50);
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gte_dup_iterate1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);
	sspagebuild b;
	ss_pagebuild_init(&b, &c);
	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 42, SVSET, &i);
	addv(&b, 80, SVSET, &j);
	addv(&b, 60, SVSET|SVDUP, &j);
	addv(&b, 41, SVSET|SVDUP, &j);
	addv(&b, 50, SVSET, &k);
	addv(&b, 40, SVSET|SVDUP, &k);
	addv(&b, 30, SVSET|SVDUP, &k);
	ss_pagebuild_end(&b);
	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	int p = 1;
	sriter it;
	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 30);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 30);
	sr_iternext(&it);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, &p, sizeof(int), 42);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	t( svlsn(v) == 42);
	sr_iternext(&it);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	t( svlsn(v) == 41);
	sr_iternext(&it);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 40);
	sr_iternext(&it);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	sr_iterinit(&it, &ss_pageiter, &c);
	sr_iteropen(&it, &page, SR_GTE, NULL, 0, 60);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == i);
	t( svlsn(v) == 42);
	sr_iternext(&it);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j);
	t( svlsn(v) == 60);
	sr_iternext(&it);
	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k);
	t( svlsn(v) == 50);
	sr_iternext(&it);
	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
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
	sspagebuild b;
	ss_pagebuild_init(&b, &c);

	t( ss_pagebuild_begin(&b, 0, 0, sizeof(int)) == 0);

	int i = 7;
	int j = 9;
	int k = 15;
	addv(&b, 5, SVSET, &i);
	addv(&b, 4, SVSET, &j);
	addv(&b, 3, SVSET|SVDUP, &j);
	addv(&b, 2, SVSET|SVDUP, &j);
	addv(&b, 1, SVSET, &k);
	ss_pagebuild_end(&b);

	sspage page;
	ss_pageinit(&page, NULL, (sspageheader*)b.kbuf.s);

	sriter it;
	sr_iterinit(&it, &ss_pageiterraw, &c);
	sr_iteropen(&it, &page);
	t( sr_iterhas(&it) != 0 );
	sv *v = sr_iterof(&it);
	t( *(int*)svkey(v) == i );
	t( svlsn(v) == 5 );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j );
	t( svlsn(v) == 4 );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j );
	t( svlsn(v) == 3 );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == j );
	t( svlsn(v) == 2 );
	sr_iternext(&it);

	t( sr_iterhas(&it) != 0 );
	v = sr_iterof(&it);
	t( *(int*)svkey(v) == k );
	t( svlsn(v) == 1 );
	sr_iternext(&it);

	t( sr_iterhas(&it) == 0 );
	v = sr_iterof(&it);
	t( v == NULL );

	ss_pagebuild_free(&b);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

int
main(int argc, char *argv[])
{
	test( test_lte_empty );
	test( test_lte_eq0 );
	test( test_lte_eq1 );
	test( test_lte_eq2 );
	test( test_lte_minmax0 );
	test( test_lte_minmax1 );
	test( test_lte_minmax2 );
	test( test_lte_mid0 );
	test( test_lte_mid1 );
	test( test_lte_iterate0 );
	test( test_lte_iterate1 );
	test( test_lt_eq );
	test( test_lt_minmax );
	test( test_lt_mid );
	test( test_lt_iterate0 );
	test( test_lt_iterate1 );
	test( test_lte_dup_eq );
	test( test_lte_dup_mid );
	test( test_lte_dup_mid_gt );
	test( test_lte_dup_mid_lt );
	test( test_lte_dup_iterate0 );
	test( test_lte_dup_iterate1 );

	test( test_gte_eq0 );
	test( test_gte_eq1 );
	test( test_gte_eq2 );
	test( test_gte_minmax0 );
	test( test_gte_minmax1 );
	test( test_gte_minmax2 );
	test( test_gte_mid0 );
	test( test_gte_mid1 );
	test( test_gte_iterate0 );
	test( test_gte_iterate1 );
	test( test_gt_eq );
	test( test_gt_minmax );
	test( test_gt_mid );
	test( test_gt_iterate0 );
	test( test_gt_iterate1 );
	test( test_gte_dup_eq );
	test( test_gte_dup_mid );
	test( test_gte_dup_mid_gt );
	test( test_gte_dup_mid_lt );
	test( test_gte_dup_iterate0 );
	test( test_gte_dup_iterate1 );

	test( test_iterate_raw );
	return 0;
}
