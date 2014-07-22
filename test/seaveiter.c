
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

static sv*
test_valloc(sra *a, svlocal *l)
{
	sv lv;
	svinit(&lv, &sv_localif, l, NULL);
	smv *mv = sm_valloc(a, &lv);
	t(mv != NULL);
	sv *v = sr_malloc(a, sizeof(sv));
	svinit(v, &sm_vif, mv, NULL);
	return v;
}

static void
test_seave(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 10)
	{
		svlocal l;
		l.lsn         = 10 - i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 20, 10, 0, NULL, 0);

	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 10 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
	}
	t( i == 10 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_limit(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 18)
	{
		svlocal l;
		l.lsn         = 18 - i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 5, 18, 0, NULL, 0);

	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
	}
	t( i == 5 );
	int j = 0;
	sv_seaveiter_resume(&seave);
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
		j++;
	}
	t( j == 5 );
	j = 0;
	sv_seaveiter_resume(&seave);
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
		j++;
	}
	t( j == 5 );
	j = 0;
	sv_seaveiter_resume(&seave);
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
		j++;
	}
	t( j == 3 );
	t( i == 18 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_limit_small(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);
	int i = 0;
	while (i < 18)
	{
		svlocal l;
		l.lsn         = 18 - i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
	}
	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 1, 18, 0, NULL, 0);

	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
	}
	t( i == 1 );
	int j = 0;
	sv_seaveiter_resume(&seave);
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
		j++;
	}
	t( j == 1 );
	j = 0;
	sv_seaveiter_resume(&seave);
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
		j++;
	}
	t( j == 1 );
	j = 0;
	sv_seaveiter_resume(&seave);
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == 18 - i );
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
		j++;
	}
	t( j == 1 );
	t( i == 4 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
addv(srbuf *list, sra *a, uint64_t lsn, int flags, char *key, int keysize)
{
	svlocal l;
	l.lsn         = lsn;
	l.flags       = flags;
	l.key         = key;
	l.keysize     = keysize;
	l.value       = NULL;
	l.valuesize   = 0;
	l.valueoffset = 0;
	sv *v = test_valloc(a, &l);
	t( sr_bufadd(list, a, &v, sizeof(sv**)) == 0 );
}

static void
checkv(sriter *i, uint64_t lsn, int flags, int key)
{
	sv *v = (sv*)sr_iterof(i);
	t( *(int*)svkey(v) == key );
	t( svlsn(v) == lsn );
	t( svflags(v) == flags );
}

static void
test_seave_dup_lsn_gt(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 10, 0, NULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVSET | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 1 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_dup_lsn_lt0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 9, 0, NULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVSET | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 2 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_dup_lsn_lt1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 8, 0, NULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVSET | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 3 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_dup_lsn_lt2(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 2, 0, NULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVSET | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 3 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_dup_lsn_gt_chain(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a, 12, SVSET, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 11, SVSET|SVDUP, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 13, SVSET, (char*)&key3, sizeof(key3));
	addv(&vlista, &a, 14, SVSET, (char*)&key4, sizeof(key4));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 15, 0, NULL, 0);

	checkv(&seave, 10, SVSET, key);
	sr_iternext(&seave);
	checkv(&seave, 12, SVSET, key2);
	sr_iternext(&seave);
	checkv(&seave, 13, SVSET, key3);
	sr_iternext(&seave);
	checkv(&seave, 14, SVSET, key4);
	sr_iternext(&seave);

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_dup_lsn_lt0_chain(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a, 12, SVSET, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 11, SVSET|SVDUP, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 13, SVSET, (char*)&key3, sizeof(key3));
	addv(&vlista, &a, 14, SVSET, (char*)&key4, sizeof(key4));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 11, 0, NULL, 0);

	checkv(&seave, 10, SVSET, key);
	sr_iternext(&seave);
	checkv(&seave, 12, SVSET, key2);
	sr_iternext(&seave);
	checkv(&seave, 11, SVSET|SVDUP, key2);
	sr_iternext(&seave);
	checkv(&seave, 13, SVSET, key3);
	sr_iternext(&seave);
	checkv(&seave, 14, SVSET, key4);
	sr_iternext(&seave);

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_dup_lsn_lt1_chain(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a, 12, SVSET, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 11, SVSET|SVDUP, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 13, SVSET, (char*)&key3, sizeof(key3));
	addv(&vlista, &a, 14, SVSET, (char*)&key4, sizeof(key4));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 9, 0, NULL, 0);

	checkv(&seave, 10, SVSET, key);
	sr_iternext(&seave);
	checkv(&seave,  9, SVSET|SVDUP, key);
	sr_iternext(&seave);
	checkv(&seave, 12, SVSET, key2);
	sr_iternext(&seave);
	checkv(&seave, 11, SVSET|SVDUP, key2);
	sr_iternext(&seave);
	checkv(&seave, 13, SVSET, key3);
	sr_iternext(&seave);
	checkv(&seave, 14, SVSET, key4);
	sr_iternext(&seave);

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_dup_lsn_lt2_chain(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;
	int key2 = 8;
	int key3 = 9;
	int key4 = 10;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a, 12, SVSET, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 11, SVSET|SVDUP, (char*)&key2, sizeof(key2));
	addv(&vlista, &a, 13, SVSET, (char*)&key3, sizeof(key3));
	addv(&vlista, &a, 14, SVSET, (char*)&key4, sizeof(key4));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 3, 0, NULL, 0);

	checkv(&seave, 10, SVSET, key);
	sr_iternext(&seave);
	checkv(&seave,  9, SVSET|SVDUP, key);
	sr_iternext(&seave);
	checkv(&seave,  8, SVSET|SVDUP, key);
	sr_iternext(&seave);
	checkv(&seave, 12, SVSET, key2);
	sr_iternext(&seave);
	checkv(&seave, 11, SVSET|SVDUP, key2);
	sr_iternext(&seave);
	checkv(&seave, 13, SVSET, key3);
	sr_iternext(&seave);
	checkv(&seave, 14, SVSET, key4);
	sr_iternext(&seave);

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_dup_lsn_limit0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 1, 15, 0, NULL, 0);

	checkv(&seave, 10, SVSET, key);
	sr_iternext(&seave);
	t( sr_iterhas(&seave) == 0 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_dup_lsn_limit1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 1, 9, 0, NULL, 0);

	checkv(&seave, 10, SVSET, key);
	sr_iternext(&seave);
	checkv(&seave,  9, SVSET|SVDUP, key);
	sr_iternext(&seave);
	t( sr_iterhas(&seave) == 0 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_dup_lsn_limit2(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;
	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVSET|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVSET|SVDUP, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 1, 5, 0, NULL, 0);

	checkv(&seave, 10, SVSET, key);
	sr_iternext(&seave);
	checkv(&seave,  9, SVSET|SVDUP, key);
	sr_iternext(&seave);
	checkv(&seave,  8, SVSET|SVDUP, key);
	sr_iternext(&seave);
	t( sr_iterhas(&seave) == 0 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_dup_lsn_limit3(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int k = 0;
	int key = 7;
	addv(&vlista, &a, 412 - k, SVSET, (char*)&key, sizeof(key));
	while (k < 411) {
		addv(&vlista, &a, 411 - k, SVSET|SVDUP, (char*)&key, sizeof(key));
		k++;
	}
	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 2, 500, 0, NULL, 0);

	t(sr_iterhas(&seave) == 1);
	checkv(&seave, 412, SVSET, key);
	sr_iternext(&seave);
	t(sr_iterhas(&seave) == 0);

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_dup_lsn_limit4(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int k = 0;
	int key = 7;
	addv(&vlista, &a, 412 - k, SVSET, (char*)&key, sizeof(key));
	while (k < 411) {
		addv(&vlista, &a, 411 - k, SVSET|SVDUP, (char*)&key, sizeof(key));
		k++;
	}
	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 1, 0, 0, NULL, 0);

	k = 0;
	while (sr_iterhas(&seave))
	{
		if (k == 0)
			checkv(&seave, 412 - k, SVSET, key);
		else
			checkv(&seave, 412 - k, SVSET|SVDUP, key);
		sr_iternext(&seave);
		k++;
	}
	t( k == 412 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_dup_lsn_limit5(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int k = 0;
	int key = 7;
	addv(&vlista, &a, 412 - k, SVSET, (char*)&key, sizeof(key));
	while (k < 411) {
		addv(&vlista, &a, 411 - k, SVSET|SVDUP, (char*)&key, sizeof(key));
		k++;
	}
	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 1, 0, 0, NULL, 0);

	k = 0;
	while (sr_iterhas(&seave))
	{
		if (k == 0)
			checkv(&seave, 412 - k, SVSET, key);
		else
			checkv(&seave, 412 - k, SVSET|SVDUP, key);
		sr_iternext(&seave);
		k++;
	}
	t( k == 412 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_delete0(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 10, 0, NULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 1 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_delete1(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 9, 0, NULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 2 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_delete2(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVSET, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 8, 0, NULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVSET );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 3 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_delete3(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 7, 0, NULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVDELETE );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 3 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_delete4(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 10, 0, NULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVDELETE );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 1 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_delete5(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 7;

	addv(&vlista, &a, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 11, 0, NULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		t( *(int*)svkey(v) == key );
		t( svlsn(v) == 10 - i );
		if (i == 0)
			t( svflags(v) == SVDELETE );
		else
			t( svflags(v) == (SVDELETE | SVDUP) );
		sr_iternext(&seave);
		i++;
	}
	t( i == 0 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_delete6(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 6;
	addv(&vlista, &a, 12, SVSET, (char*)&key, sizeof(key));
	key = 7;
	addv(&vlista, &a, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	key = 10;
	addv(&vlista, &a, 11, SVSET, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 13, 0, NULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		if (i == 0) {
			t( *(int*)svkey(v) == 6 );
			t( svlsn(v) == 12 );
		} else {
			t( *(int*)svkey(v) == 10 );
			t( svlsn(v) == 11 );
		}
		t( svflags(v) == SVSET );
		sr_iternext(&seave);
		i++;
	}
	t( i == 2 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_delete7(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 6;
	addv(&vlista, &a, 12, SVSET, (char*)&key, sizeof(key));
	key = 7;
	addv(&vlista, &a, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	key = 10;
	addv(&vlista, &a, 11, SVSET, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 10, 0, NULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		if (i == 0) {
			t( *(int*)svkey(v) == 6 );
			t( svflags(v) == SVSET );
			t( svlsn(v) == 12 );
		} else
		if (i == 1) {
			t( *(int*)svkey(v) == 7 );
			t( svflags(v) == SVDELETE );
			t( svlsn(v) == 10 );
		} else {
			t( *(int*)svkey(v) == 10 );
			t( svflags(v) == SVSET );
			t( svlsn(v) == 11 );
		}
		sr_iternext(&seave);
		i++;
	}
	t( i == 3 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_seave_delete8(void)
{
	sra a;
	sr_allocinit(&a, sr_allocstd, NULL);
	srcmpindex ci;
	sr_cmpindex_init(&ci, &a);
	srscheme sdb;
	sr_schemeinit(&sdb, &a, "test", &ci, "u32");
	src c;
	sr_cinit(&c, NULL, &sdb, NULL, &a);

	srbuf vlista;
	srbuf vlistb;
	sr_bufinit(&vlista);
	sr_bufinit(&vlistb);

	int key = 6;
	addv(&vlista, &a, 12, SVSET, (char*)&key, sizeof(key));
	key = 7;
	addv(&vlista, &a, 10, SVDELETE, (char*)&key, sizeof(key));
	addv(&vlista, &a,  9, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	addv(&vlista, &a,  8, SVDELETE|SVDUP, (char*)&key, sizeof(key));
	key = 10;
	addv(&vlista, &a, 11, SVSET, (char*)&key, sizeof(key));

	sriter ita;
	sr_iterinit(&ita, &sr_bufiterref, &c);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	sriter itb;
	sr_iterinit(&itb, &sr_bufiterref, &c);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));

	sriter merge;
	sr_iterinit(&merge, &sv_mergeiter, &c);
	sr_iteropen(&merge, &ita, &itb);

	sriter seave;
	sr_iterinit(&seave, &sv_seaveiter, &c);
	sr_iteropen(&seave, &merge, 10, 9, 0, NULL, 0);

	int i = 0;
	i = 0;
	while (sr_iterhas(&seave)) {
		sv *v = (sv*)sr_iterof(&seave);
		if (i == 0) {
			t( *(int*)svkey(v) == 6 );
			t( svflags(v) == SVSET );
			t( svlsn(v) == 12 );
		} else
		if (i == 1) {
			t( *(int*)svkey(v) == 7 );
			t( svflags(v) == SVDELETE );
			t( svlsn(v) == 10 );
		} else
		if (i == 2) {
			t( *(int*)svkey(v) == 7 );
			t( svflags(v) == (SVDELETE|SVDUP) );
			t( svlsn(v) ==  9 );
		} else {
			t( *(int*)svkey(v) == 10 );
			t( svflags(v) == SVSET );
			t( svlsn(v) == 11 );
		}
		sr_iternext(&seave);
		i++;
	}
	t( i == 4 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_buffree(&vlista, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

int
main(int argc, char *argv[])
{
	test( test_seave );
	test( test_seave_limit );
	test( test_seave_limit_small );
	test( test_seave_dup_lsn_gt );
	test( test_seave_dup_lsn_lt0 );
	test( test_seave_dup_lsn_lt1 );
	test( test_seave_dup_lsn_lt2 );
	test( test_seave_dup_lsn_gt_chain );
	test( test_seave_dup_lsn_lt0_chain );
	test( test_seave_dup_lsn_lt1_chain );
	test( test_seave_dup_lsn_lt2_chain );
	test( test_seave_dup_lsn_limit0 );
	test( test_seave_dup_lsn_limit1 );
	test( test_seave_dup_lsn_limit2 );
	test( test_seave_dup_lsn_limit3 );
	test( test_seave_dup_lsn_limit4 );
	test( test_seave_dup_lsn_limit5 );
	test( test_seave_delete0 );
	test( test_seave_delete1 );
	test( test_seave_delete2 );
	test( test_seave_delete3 );
	test( test_seave_delete4 );
	test( test_seave_delete5 );
	test( test_seave_delete6 );
	test( test_seave_delete7 );
	test( test_seave_delete8 );
	return 0;
}
