
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
test_merge_a(void)
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
		l.lsn         = i,
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
	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == i );
		t( svflags(v) == SVSET );
		sr_iternext(&merge);
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
test_merge_b(void)
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
		l.lsn         = i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
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
	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == i );
		t( svflags(v) == SVSET );
		sr_iternext(&merge);
		i++;
	}
	t( i == 10 );

	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlistb, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_ab(void)
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
	while (i < 5)
	{
		svlocal l;
		l.lsn         = i,
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
	while (i < 10)
	{
		svlocal l;
		l.lsn         = i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
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
	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == i );
		t( svflags(v) == SVSET );
		sr_iternext(&merge);
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
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_ba(void)
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
	while (i < 5)
	{
		svlocal l;
		l.lsn         = i,
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
	while (i < 10)
	{
		svlocal l;
		l.lsn         = i,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
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
	sr_iteropen(&merge, &itb, &ita);
	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)svkey(v) == i );
		t( svlsn(v) == i );
		t( svflags(v) == SVSET );
		sr_iternext(&merge);
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
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_dup_ab(void)
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
	int lsn = 10;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
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

	int key = 0;
	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		if ((i % 2) == 0) {
			t( *(int*)svkey(v) == key );
			t( svflags(v) == SVSET );
			key++;
		} else {
			t( *(int*)svkey(v) == key - 1);
			t( svflags(v) == (SVSET | SVDUP) );
		}
		sr_iternext(&merge);
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
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_dup_a_chain(void)
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
	int lsn = 5;
	int i = 0;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET | ((i > 0) ? SVDUP: 0),
		l.key         = &key;
		l.keysize     = sizeof(key);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
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

	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)svkey(v) == key );
		if (i == 0) {
			t( svflags(v) == SVSET );
		} else {
			t( svflags(v) == (SVSET | SVDUP) );
		}
		sr_iternext(&merge);
		i++;
	}
	t( i == 5 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_dup_ab_chain(void)
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
	int lsn = 10;
	int key = 7;
	int i = 0;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET | ((i > 0) ? SVDUP: 0),
		l.key         = &key;
		l.keysize     = sizeof(key);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
	}
	i = 0 ;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET | ((i > 0) ? SVDUP: 0),
		l.key         = &key;
		l.keysize     = sizeof(key);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn--;
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

	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)svkey(v) == key );
		if (i == 0) {
			t( svflags(v) == SVSET );
		} else {
			t( svflags(v) == (SVSET | SVDUP) );
		}
		sr_iternext(&merge);
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
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

#if 0
static void
test_merge_dup_ab_recover(void)
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
	int lsn = 0;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn++;
	}
	i = 0 ;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET,
		l.key         = &i;
		l.keysize     = sizeof(i);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn++;
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

	lsn = 5;
	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( svflags(v) == SVSET );
		t( *(int*)svkey(v) == i);
		t( svlsn(v) == 5 + i);
		sr_iternext(&merge);
		i++;
	}
	t( i == 5 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}

static void
test_merge_dup_ab_chain_recover(void)
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
	int lsn = 0;
	int key = 7;
	int i = 0;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET | ((i > 0) ? SVDUP: 0),
		l.key         = &key;
		l.keysize     = sizeof(key);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlista, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn++;
	}
	i = 0 ;
	while (i < 5)
	{
		svlocal l;
		l.lsn         = lsn,
		l.flags       = SVSET | ((i > 0) ? SVDUP: 0),
		l.key         = &key;
		l.keysize     = sizeof(key);
		l.value       = NULL;
		l.valuesize   = 0;
		l.valueoffset = 0;
		sv *v = test_valloc(&a, &l);
		t(sr_bufadd(&vlistb, &a, &v, sizeof(sv**)) == 0);
		i++;
		lsn++;
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

	i = 0;
	while (sr_iterhas(&merge)) {
		sv *v = (sv*)sr_iterof(&merge);
		t( *(int*)svkey(v) == key );
		if (i == 0) {
			t( svflags(v) == SVSET );
		} else {
			t( svflags(v) == (SVSET | SVDUP) );
		}
		sr_iternext(&merge);
		i++;
	}
	t( i == 5 );

	sr_iterinit(&ita, &sr_bufiterref, NULL);
	sr_iteropen(&ita, &vlista, sizeof(sv*));
	while (sr_iterhas(&ita)) {
		sv *v = (sv*)sr_iterof(&ita);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&ita);
	}
	sr_iterinit(&itb, &sr_bufiterref, NULL);
	sr_iteropen(&itb, &vlistb, sizeof(sv*));
	while (sr_iterhas(&itb)) {
		sv *v = (sv*)sr_iterof(&itb);
		sr_free(&a, v->v);
		sr_free(&a, v);
		sr_iternext(&itb);
	}
	sr_buffree(&vlista, &a);
	sr_buffree(&vlistb, &a);
	sr_schemefree(&sdb, &a);
	sr_cmpindex_free(&ci, &a);
}
#endif

int
main(int argc, char *argv[])
{
	test( test_merge_a );
	test( test_merge_b );
	test( test_merge_ab );
	test( test_merge_ba );
	test( test_merge_dup_ab );
	test( test_merge_dup_a_chain );
	test( test_merge_dup_ab_chain );
	/*
	test( test_merge_dup_ab_recover );
	test( test_merge_dup_ab_chain_recover );
	*/
	return 0;
}
