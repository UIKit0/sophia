
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsl.h>
#include <libss.h>
#include <libsm.h>
#include <libsd.h>

int sd_queryopen(sdquery *q, sd *db, srorder order, uint64_t lsvn,
                 void *key, int keysize)
{
	q->order     = order;
	q->db        = db;
	q->n         = NULL;
	q->lsvn      = lsvn;
	q->key       = key;
	q->keysize   = keysize;
	q->value     = NULL;
	q->valuesize = 0;
	q->rbuf      = NULL;
	memset(&q->origin, 0, sizeof(q->origin));
	memset(&q->result, 0, sizeof(q->result));
	return 0;
}

int sd_queryclose(sdquery *q)
{
	if (q->n) {
		sd_nodeunlock(q->n);
		q->n = NULL;
	}
	return 0;
}

int sd_querybuf(sdquery *q, srbuf *rbuf)
{
	q->rbuf = rbuf;
	return 0;
}

static void
sd_min(sdquery *q, smv *a, sv *b)
{
	if (a && b) {
		int rc = sr_compare(&q->db->c->sdb->cmp, a->key, a->keysize,
		                    svkey(b), svkeysize(b));
		switch (rc) {
		case -1:
		case  0: svinit(&q->result, &sm_vif, a, NULL);
			break;
		case  1: q->result = *b;
			break;
		}
		return;
	}
	if (a) {
		svinit(&q->result, &sm_vif, a, NULL);
		return;
	}
	if (b) {
		q->result = *b;
	}
}

static void
sd_max(sdquery *q, smv *a, sv *b)
{
	if (a && b) {
		int rc = sr_compare(&q->db->c->sdb->cmp, a->key, a->keysize,
		                    svkey(b), svkeysize(b));
		switch (rc) {
		case  1:
		case  0: svinit(&q->result, &sm_vif, a, NULL);
			break;
		case -1: q->result = *b;
			break;
		}
		return;
	}
	if (a) {
		svinit(&q->result, &sm_vif, a, NULL);
		return;
	}
	if (b) {
		q->result = *b;
	}
}

static int
sd_fetch(sdquery *q)
{
	sd *db = q->db;

	/* match node */
	sd_indexlock(&db->primary);
	sriter r;
	sr_iterinit(&r, &sd_indexiter, db->c);
	sr_iteropen(&r, &db->primary, q->order, q->key, q->keysize);
	sdnode *n = sr_iterof(&r);
	assert(n != NULL);
	sd_nodelock(n);
	q->n = n;
	sd_indexunlock(&db->primary);

	/* match page */
	sriter i;
	sr_iterinit(&i, &ss_indexiter, n->c);
	sr_iteropen(&i, &n->index, q->order, q->key, q->keysize, q->lsvn);

	/* read page */
	ssref *page = sr_iterof(&i);
	sv *v = NULL;
	if (srlikely(page)) {
		int rc = ss_read(q->db->store, n->c, q->rbuf,
		                 &q->origin, page, NULL);
		if (srunlikely(rc == -1))
			return -1;
		/* match page key */
		sriter j;
		sr_iterinit(&j, &ss_pageiter, n->c);
		sr_iteropen(&j, &q->origin, q->order, q->key, q->keysize, q->lsvn);
		v = sr_iterof(&j);
	}

	/* compare page key with in-memory versions */
	sdpairquery pq;
	sd_pairquery_open(&pq, &n->ip, n->c, q->order,
	                  q->lsvn,
	                  q->key, q->keysize);
	sd_pairquery(&pq);
	switch (q->order) {
	case SR_LT:
	case SR_LTE: sd_max(q, pq.v, v);
		break;
	case SR_GT:
	case SR_GTE: sd_min(q, pq.v, v);
		break;
	default: assert(0);
	}
	sd_pairquery_close(&pq);

	return q->result.v != NULL;
}

static inline int
sd_match(sdquery *q)
{
	sd *db = q->db;

	/* match node */
	sd_indexlock(&db->primary);
	sriter r;
	sr_iterinit(&r, &sd_indexiter, db->c);
	sr_iteropen(&r, &db->primary, q->order, q->key, q->keysize);
	sdnode *n = sr_iterof(&r);
	assert(n != NULL);
	sd_nodelock(n);
	q->n = n;
	sd_indexunlock(&db->primary);

	/* search in-memory index pair */
	sdpairquery pq;
	sd_pairquery_open(&pq, &n->ip, n->c, q->order,
	                  q->lsvn,
	                  q->key, q->keysize);
	int rc = sd_pairquery(&pq);
	if (rc) {
		svinit(&q->result, &sm_vif, pq.v, NULL);
		sd_pairquery_close(&pq);
		return 1;
	}
	sd_pairquery_close(&pq);

	/* match index page */
	sriter i;
	sr_iterinit(&i, &ss_indexiter, n->c);
	int eq = sr_iteropen(&i, &n->index, q->order,
	                     q->key, q->keysize, q->lsvn);
	if (! sr_iterhas(&i))
		return 0;
	if (! eq)
		return 0;

	/* read page */
	ssref *page = sr_iterof(&i);
	assert(page != NULL);
	rc = ss_read(q->db->store, n->c, q->rbuf, &q->origin,
	             page, NULL);
	if (srunlikely(rc == -1))
		return -1;

	/* match page key */
	sriter j;
	sr_iterinit(&j, &ss_pageiter, n->c);
	rc = sr_iteropen(&j, &q->origin, q->order, q->key,
	                 q->keysize, q->lsvn);
	if (! rc)
		return 0;

	sv *match = sr_iterof(&j);
	assert(match != NULL);
	q->result = *match;
	return rc;
}

int sd_query(sdquery *q)
{
	switch (q->order) {
	case SR_LT:
	case SR_LTE:
	case SR_GT:
	case SR_GTE:    return sd_fetch(q);
		break;
	case SR_EQ:
	case SR_UPDATE: return sd_match(q);
	default: break;
	}
	assert(0);
	return 0;
}

int sd_pagecommited(sd *db, sv *v)
{
	/* match node */
	sd_indexlock(&db->primary);
	sriter r;
	sr_iterinit(&r, &sd_indexiter, db->c);
	sr_iteropen(&r, &db->primary, SR_LTE, svkey(v), svkeysize(v));
	sdnode *n = sr_iterof(&r);
	assert(n != NULL);
	sd_nodelock(n);
	sd_indexunlock(&db->primary);

	/* match index page */
	sriter i;
	sr_iterinit(&i, &ss_indexiter, n->c);
	int eq = sr_iteropen(&i, &n->index, SR_LTE,
	                     svkey(v), svkeysize(v),
	                     UINT64_MAX);
	if (! sr_iterhas(&i))
		goto not_found;
	if (! eq)
		goto not_found;
	ssref *page = sr_iterof(&i);
	int rc = page->lsnmax >= svlsn(v);
	sd_nodeunlock(n);
	return rc;
not_found:
	sd_nodeunlock(n);
	return 0;
}
