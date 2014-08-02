
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

int sd_pairinit(sdpair *p, src *c)
{
	int rc = sm_indexinit(&p->a, c);
	if (srunlikely(rc == -1))
		return -1;
	rc = sm_indexinit(&p->b, c);
	if (srunlikely(rc == -1)) {
		sm_indexfree(&p->a);
		return -1;
	}
	p->i = &p->a;
	return 0;
}

int sd_pairfree(sdpair *p, int index_only)
{
	if (index_only) {
		sm_indexfreei(&p->a);
		sm_indexfreei(&p->b);
	} else {
		sm_indexfree(&p->a);
		sm_indexfree(&p->b);
	}
	return 0;
}

int sd_pairgc(sdpair *p)
{
	sm_indexfreei(p->i);
	smindex *old = sd_pairshadow(p);
	sm_indexfree(old);
	return 0;
}

int sd_pairclean(sdpair *p, src *c, smindex *i)
{
	smindex copy = *i;
	smindex n;
	int rc = sm_indexinit(&n, c);
	if (srunlikely(rc == -1))
		return -1;
	(void)p;
	/*sr_spinlock(&p->lock);*/
	*i = n;
	/*sr_spinunlock(&p->lock);*/
	sm_indexfree(&copy);
	return 0;
}

int
sd_pairquery_open(sdpairquery *q, sdpair *p, src *c, srorder order,
                  uint64_t lsvn, void *key, int keysize)
{
	q->p       = p;
	q->c       = c;
	q->order   = order;
	q->key     = key;
	q->keysize = keysize;
	q->lsvn    = lsvn;
	q->v       = NULL;
	return 0;
}

int sd_pairquery_close(sdpairquery *q)
{
	q->v = NULL;
	return 0;
}

static inline int
sd_pairlt(sdpairquery *q)
{
	sriter i;
	sr_iterinit(&i, &sm_indexiter, q->c);
	sr_iteropen(&i, q->p->i, q->order, q->key, q->keysize, q->lsvn);
	sv *a = sr_iterof(&i);

	sriter j;
	sr_iterinit(&j, &sm_indexiter, q->c);
	sr_iteropen(&j, sd_pairshadow(q->p), q->order, q->key, q->keysize, q->lsvn);
	sv *b = sr_iterof(&j);

	sv *v = a;
	if (a && b) {
		int rc = svcompare(a, b, &q->c->sdb->cmp);
		switch (rc) {
		case  1:
		case  0: v = a;
			break;
		case -1: v = b;
			break;
		}
	} else
	if (b) {
		v = b;
	}

	if (v) {
		q->v = v->v;
	}
	return q->v != NULL;
}

static inline int
sd_pairgt(sdpairquery *q)
{
	sriter i;
	sr_iterinit(&i, &sm_indexiter, q->c);
	sr_iteropen(&i, q->p->i, q->order, q->key, q->keysize, q->lsvn);
	sv *a = sr_iterof(&i);

	sriter j;
	sr_iterinit(&j, &sm_indexiter, q->c);
	sr_iteropen(&j, sd_pairshadow(q->p), q->order, q->key, q->keysize, q->lsvn);
	sv *b = sr_iterof(&j);

	sv *v = a;
	if (a && b) {
		int rc = svcompare(a, b, &q->c->sdb->cmp);
		switch (rc) {
		case -1:
		case  0: v = a;
			break;
		case  1: v = b;
			break;
		}

	} else
	if (b) {
		v = b;
	}

	if (v) {
		q->v = v->v;
	}
	return q->v != NULL;
}

static inline int
sd_paireq(sdpairquery *q)
{
	q->v = sm_indexmatch(q->p->i, q->lsvn, q->key, q->keysize);
	if (q->v)
		return 1;
	q->v = sm_indexmatch(sd_pairshadow(q->p), q->lsvn, q->key, q->keysize);
	return q->v != NULL;
}

static inline int
sd_pairupdated(sdpairquery *q)
{
	q->v = sm_indexmatchhead(q->p->i, q->key, q->keysize);
	if (q->v)
		return q->v->id.lsn > q->lsvn;
	q->v = sm_indexmatchhead(sd_pairshadow(q->p), q->key, q->keysize);
	if (q->v)
		return q->v->id.lsn > q->lsvn;
	return 0;
}

int sd_pairquery(sdpairquery *q)
{
	switch (q->order) {
	case SR_LT:
	case SR_LTE:    return sd_pairlt(q);
	case SR_GT:
	case SR_GTE:    return sd_pairgt(q);
	case SR_EQ:     return sd_paireq(q);
	case SR_UPDATE: return sd_pairupdated(q);
		break;
	}
	return 0;
}
