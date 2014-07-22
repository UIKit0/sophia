
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
test_gt0(void)
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
	int key = 7;
	addv(&b, 3, SVSET, &key);
	key = 8;
	addv(&b, 4, SVSET, &key);
	key = 9;
	addv(&b, 5, SVSET, &key);
	ss_pagebuild_end(&b);
	ss_pagebuild_commit(&b);
	srbuf buf;
	sr_bufinit(&buf);
	t( ss_pagebuild_write(&b, &buf) == 0 );

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sr_filewrite(&f, buf.s, sr_bufused(&buf)) == 0 );

	sriter it;
	sr_iterinit(&it, &ss_dbiter, &c);
	sr_iteropen(&it, &f, SR_GT, 1);
	t( sr_iterhas(&it) == 1 );

	sspage *page = sr_iterof(&it);
	t( page != NULL );

	sriter pi;
	sr_iterinit(&pi, &ss_pageiter, &c);
	sr_iteropen(&pi, page, SR_GT, NULL, 0, 5);
	t( sr_iterhas(&pi) != 0 );
	sv *v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 7);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 8);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 9);
	sr_iternext(&pi);
	t( sr_iterhas(&pi) == 0 );

	sr_iternext(&it);
	t( sr_iterhas(&it) == 0 );

	sr_fileclose(&f);
	t( sr_fileunlink("./0000.db") == 0 );

	ss_pagebuild_free(&b);
	sr_buffree(&buf, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_gt1(void)
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
	int key = 7;
	addv(&b, 3, SVSET, &key);
	key = 8;
	addv(&b, 4, SVSET, &key);
	key = 9;
	addv(&b, 5, SVSET, &key);
	ss_pagebuild_end(&b);
	ss_pagebuild_commit(&b);

	t( ss_pagebuild_begin(&b, 0, 1, sizeof(int)) == 0);
	key = 10;
	addv(&b, 6, SVSET, &key);
	key = 11;
	addv(&b, 7, SVSET, &key);
	key = 13;
	addv(&b, 8, SVSET, &key);
	ss_pagebuild_end(&b);
	ss_pagebuild_commit(&b);

	t( ss_pagebuild_begin(&b, 0, 2, sizeof(int)) == 0);
	key = 15;
	addv(&b, 9, SVSET, &key);
	key = 18;
	addv(&b, 10, SVSET, &key);
	key = 20;
	addv(&b, 11, SVSET, &key);
	ss_pagebuild_end(&b);
	ss_pagebuild_commit(&b);

	srbuf buf;
	sr_bufinit(&buf);
	t( ss_pagebuild_write(&b, &buf) == 0 );

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sr_filewrite(&f, buf.s, sr_bufused(&buf)) == 0 );

	sriter it;
	sr_iterinit(&it, &ss_dbiter, &c);
	sr_iteropen(&it, &f, SR_GT, 1);
	t( sr_iterhas(&it) == 1 );

	sspage *page = sr_iterof(&it);
	t( page != NULL );

	sriter pi;
	sr_iterinit(&pi, &ss_pageiter, &c);
	sr_iteropen(&pi, page, SR_GT, NULL, 0, 100);
	t( sr_iterhas(&pi) != 0 );
	sv *v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 7);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 8);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 9);
	sr_iternext(&pi);
	t( sr_iterhas(&pi) == 0 );

	sr_iternext(&it);
	t( sr_iterhas(&it) == 1 );
	page = sr_iterof(&it);
	t( page != NULL );

	sr_iterinit(&pi, &ss_pageiter, &c);
	sr_iteropen(&pi, page, SR_GT, NULL, 0, 100);
	t( sr_iterhas(&pi) != 0 );
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 10);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 11);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 13);
	sr_iternext(&pi);
	t( sr_iterhas(&pi) == 0 );

	sr_iternext(&it);
	t( sr_iterhas(&it) == 1 );
	page = sr_iterof(&it);
	t( page != NULL );

	sr_iterinit(&pi, &ss_pageiter, &c);
	sr_iteropen(&pi, page, SR_GT, NULL, 0, 100);
	t( sr_iterhas(&pi) != 0 );
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 15);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 18);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 20);
	sr_iternext(&pi);
	t( sr_iterhas(&pi) == 0 );

	sr_iternext(&it);
	t( sr_iterhas(&it) == 0 );
	page = sr_iterof(&it);
	t( page == NULL );

	sr_fileclose(&f);
	t( sr_fileunlink("./0000.db") == 0 );

	ss_pagebuild_free(&b);
	sr_buffree(&buf, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lt0(void)
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
	int key = 7;
	addv(&b, 3, SVSET, &key);
	key = 8;
	addv(&b, 4, SVSET, &key);
	key = 9;
	addv(&b, 5, SVSET, &key);
	ss_pagebuild_end(&b);
	ss_pagebuild_commit(&b);
	srbuf buf;
	sr_bufinit(&buf);
	t( ss_pagebuild_write(&b, &buf) == 0 );

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sr_filewrite(&f, buf.s, sr_bufused(&buf)) == 0 );

	sriter it;
	sr_iterinit(&it, &ss_dbiter, &c);
	sr_iteropen(&it, &f, SR_LT, 1);
	t( sr_iterhas(&it) == 1 );

	sspage *page = sr_iterof(&it);
	t( page != NULL );

	sriter pi;
	sr_iterinit(&pi, &ss_pageiter, &c);
	sr_iteropen(&pi, page, SR_GT, NULL, 0, 5);
	t( sr_iterhas(&pi) != 0 );
	sv *v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 7);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 8);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 9);
	sr_iternext(&pi);
	t( sr_iterhas(&pi) == 0 );

	sr_iternext(&it);
	t( sr_iterhas(&it) == 0 );

	sr_fileclose(&f);
	t( sr_fileunlink("./0000.db") == 0 );

	ss_pagebuild_free(&b);
	sr_buffree(&buf, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_lt1(void)
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
	int key = 7;
	addv(&b, 3, SVSET, &key);
	key = 8;
	addv(&b, 4, SVSET, &key);
	key = 9;
	addv(&b, 5, SVSET, &key);
	ss_pagebuild_end(&b);
	ss_pagebuild_commit(&b);

	t( ss_pagebuild_begin(&b, 0, 1, sizeof(int)) == 0);
	key = 10;
	addv(&b, 6, SVSET, &key);
	key = 11;
	addv(&b, 7, SVSET, &key);
	key = 13;
	addv(&b, 8, SVSET, &key);
	ss_pagebuild_end(&b);
	ss_pagebuild_commit(&b);

	t( ss_pagebuild_begin(&b, 0, 2, sizeof(int)) == 0);
	key = 15;
	addv(&b, 9, SVSET, &key);
	key = 18;
	addv(&b, 10, SVSET, &key);
	key = 20;
	addv(&b, 11, SVSET, &key);
	ss_pagebuild_end(&b);
	ss_pagebuild_commit(&b);

	srbuf buf;
	sr_bufinit(&buf);
	t( ss_pagebuild_write(&b, &buf) == 0 );

	srfile f;
	sr_fileinit(&f, &a);
	t( sr_filenew(&f, "./0000.db") == 0 );
	t( sr_filewrite(&f, buf.s, sr_bufused(&buf)) == 0 );

	sriter it;
	sr_iterinit(&it, &ss_dbiter, &c);
	sr_iteropen(&it, &f, SR_LT, 1);
	t( sr_iterhas(&it) == 1 );

	sspage *page = sr_iterof(&it);
	t( page != NULL );

	sriter pi;
	sr_iterinit(&pi, &ss_pageiter, &c);
	sr_iteropen(&pi, page, SR_GT, NULL, 0, 100);
	t( sr_iterhas(&pi) != 0 );
	sv *v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 15);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 18);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 20);
	sr_iternext(&pi);
	t( sr_iterhas(&pi) == 0 );

	sr_iternext(&it);
	t( sr_iterhas(&it) == 1 );
	page = sr_iterof(&it);
	t( page != NULL );

	sr_iterinit(&pi, &ss_pageiter, &c);
	sr_iteropen(&pi, page, SR_GT, NULL, 0, 100);
	t( sr_iterhas(&pi) != 0 );
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 10);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 11);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 13);
	sr_iternext(&pi);
	t( sr_iterhas(&pi) == 0 );

	sr_iternext(&it);
	t( sr_iterhas(&it) == 1 );
	page = sr_iterof(&it);
	t( page != NULL );

	sr_iterinit(&pi, &ss_pageiter, &c);
	sr_iteropen(&pi, page, SR_GT, NULL, 0, 100);
	t( sr_iterhas(&pi) != 0 );
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 7);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 8);
	sr_iternext(&pi);
	v = sr_iterof(&pi);
	t( *(int*)svkey(v) == 9);
	sr_iternext(&pi);
	t( sr_iterhas(&pi) == 0 );

	sr_iternext(&it);
	t( sr_iterhas(&it) == 0 );
	page = sr_iterof(&it);
	t( page == NULL );

	sr_fileclose(&f);
	t( sr_fileunlink("./0000.db") == 0 );

	ss_pagebuild_free(&b);
	sr_buffree(&buf, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

int
main(int argc, char *argv[])
{
	test( test_gt0 );
	test( test_gt1 );
	test( test_lt0 );
	test( test_lt1 );
	return 0;
}
