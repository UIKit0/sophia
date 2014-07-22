
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libss.h>

int
ss_mergeinit(ssmerge *m, ss *s, src *c, ssc *sc, uint32_t dsn,
             ssindex *index,
             sriter *stream, uint32_t stream_keysize,
             uint64_t lsvn)
{
	m->s       = s;
	m->index   = index;
	m->stream  = stream;
	m->dsn     = dsn;
	m->lsvn    = lsvn;
	m->c       = c;
	m->keysize = stream_keysize;
	if (ss_indexhas(index)) {
		if (index->keymax > m->keysize)
			m->keysize = index->keymax;
	}
	ss_indexinit(&m->result, m->keysize);
	ss_creset(sc);
	m->b       = &sc->build;
	m->rbuf    = &sc->a;
	m->gc      = &sc->b;
	return 0;
}

int ss_mergefree(ssmerge *m)
{
	ss_indexfree(&m->result, m->c->a);
	return 0;
}

static inline int
ss_mergemark(ssmerge *m, ssdb *db)
{
	if (srunlikely(db == NULL))
		return 0;
	sriter i;
	sr_iterinit(&i, &sr_bufiter, NULL);
	sr_iteropen(&i, m->gc, sizeof(ssmergegc));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		ssmergegc *gc = sr_iterof(&i);
		if (gc->db == db) {
			gc->sweep++;
			return 0;
		}
	}
	int rc = sr_bufensure(m->gc, m->c->a, sizeof(ssmergegc));
	if (srunlikely(rc == -1))
		return -1;
	ssmergegc *mgc = (ssmergegc*)m->gc->p;
	mgc->db    = db;
	mgc->sweep = 1;
	sr_bufadvance(m->gc, sizeof(ssmergegc));
	return 0;
}

static inline int
ss_mergesweep(ssmerge *m)
{
	sriter i;
	sr_iterinit(&i, &sr_bufiter, NULL);
	sr_iteropen(&i, m->gc, sizeof(ssmergegc));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		ssmergegc *gc = sr_iterof(&i);
		sr_gcsweep(&gc->db->gc, gc->sweep);
	}
	return 0;
}

static inline int
ss_mergepage_read(ssmerge *m, sspage *origin, ssref *page, ssdb **db)
{
	int rc;
	if (srlikely(page))
		return ss_read(m->s, m->c, m->rbuf, origin, page, db);
	rc = sr_bufensure(m->rbuf, m->c->a, sizeof(sspageheader));
	if (srunlikely(rc == -1))
		return -1;
	sspageheader *h = (sspageheader*)m->rbuf->s;
	memset(h, 0, sizeof(*h));
	h->psn = sr_seq(m->c->seq, SR_PSNNEXT);
	ss_pageinit(origin, NULL, h);
	return 0;
}

static int
ss_mergepage(ssmerge *m, ssref *page, int bound)
{
	/* read page */
	char *max = NULL;
	int maxsize = 0;
	if (srlikely(page)) {
		max = ss_refmax(page);
		maxsize = page->sizemax;
	}
	ssdb *sdb = NULL;
	sspage origin;
	int rc = ss_mergepage_read(m, &origin, page, &sdb);
	if (srunlikely(rc == -1))
		return -1;

	/* merge page with key stream */
	sriter j;
	sr_iterinit(&j, &ss_pageiterraw, m->c);
	sr_iteropen(&j, &origin);
	sriter k;
	sr_iterinit(&k, &sv_mergeiter, m->c);
	sr_iteropen(&k, m->stream, &j);
	sriter g;
	sr_iterinit(&g, &sv_seaveiter, m->c);
	sr_iteropen(&g, &k, m->c->c->page_size, m->lsvn,
	            bound, max, maxsize);

	int split = 0;
	int count = 0;
	for (;;)
	{
		uint32_t psn = (split) ?
		      sr_seq(m->c->seq, SR_PSNNEXT) :
		      origin.h->psn;
		rc = ss_pagebuild_begin(m->b, m->dsn, psn, m->keysize);
		if (srunlikely(rc == -1))
			return -1;
		count = 0;
		while (sr_iterhas(&g)) {
			sv *v = sr_iterof(&g);
			rc = ss_pagebuild_add(m->b, v);
			if (srunlikely(rc == -1))
				return -1;
			sr_iternext(&g);
			count++;
		}
		if (srunlikely(count == 0)) {
			ss_pagebuild_rollback(m->b);
			/* write page delete */
			if (srunlikely(split == 0 && page)) {
				rc = ss_pagebuild_delete(m->b, m->dsn, psn);
				if (srunlikely(rc == -1))
					return -1;
			}
			break;
		}
		rc = ss_pagebuild_end(m->b);
		if (srunlikely(rc == -1))
			return -1;
		ssref *p = ss_pagebuild_ref(m->b, m->c->a, psn);
		if (srunlikely(p == NULL))
			return -1;
		rc = ss_indexadd(&m->result, m->c->a, p);
		if (srunlikely(rc == -1)) {
			sr_free(m->c->a, p);
			return -1;
		}

		ss_pagebuild_commit(m->b);
		split++;

		/* bound reach */
		if (sv_seaveiter_resume(&g))
			break;
	}

	rc = ss_mergemark(m, sdb);
	if (srunlikely(rc == -1))
		return -1;
	sr_bufreset(m->rbuf);
	return 0;
}

int ss_merge(ssmerge *m)
{
	sriter i;
	sr_iterinit(&i, &ss_indexiterraw, m->c);
	sr_iteropen(&i, m->index);
	if (srunlikely(! sr_iterhas(&i)))
		return ss_mergepage(m, NULL, 0);

	ssref *max = ss_indexmax(m->index);
	int rc;

	while (sr_iterhas(&i) && sr_iterhas(m->stream))
	{
		ssref *page = sr_iterof(&i);
		sv *key = sr_iterof(m->stream);

		if (srunlikely(page == max)) {
			rc = ss_mergepage(m, page, 0);
			if (srunlikely(rc == -1))
				return -1;
			sr_iternext(&i);
			break;
		}
		rc = ss_refcmp(page, svkey(key), svkeysize(key),
		               m->c->sdb->cmp);
		switch (rc) {
		case -1: /* [page] key */
			ss_indexaccount(&m->result, page);
			rc = ss_indexadd(&m->result, m->c->a, page);
			break;
		case  1: /* key [page] */
		case  0: /* [page key] */
			rc = ss_mergepage(m, page, 1);
			break;
		}
		if (srunlikely(rc == -1))
			return -1;

		sr_iternext(&i);
	}

	assert(sr_iterhas(m->stream) == 0);

	while (sr_iterhas(&i)) {
		ssref *page = sr_iterof(&i);
		rc = ss_indexadd(&m->result, m->c->a, page);
		if (srunlikely(rc == -1))
			return -1;
		ss_indexaccount(&m->result, page);
		sr_iternext(&i);
	}

	return 0;
}

int ss_mergewrite(ssmerge *m, sswritef cb, void *cbarg)
{
	sswrite w;
	ss_writeinit(&w, 1, &m->result, m->b, NULL, 0);
	ss_writeinit_callback(&w, cb, cbarg);
	int rc = ss_write(m->s, &w);
	if (srunlikely(rc == -1))
		return -1;
	ss_mergesweep(m);
	return 0;
}
