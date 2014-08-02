
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

typedef struct sdindexiter sdindexiter;

struct sdindexiter {
	sdnodeindex *index;
	sdnode *v;
	int pos;
	srorder order;
	void *key;
	int keysize;
} srpacked;

static void
sd_indexiter_init(sriter *i)
{
	assert(sizeof(sdindexiter) <= sizeof(i->priv));
	sdindexiter *ii = (sdindexiter*)i->priv;
	memset(ii, 0, sizeof(*ii));
}

static inline int
sd_indexiter_seek(sdindexiter *i, int *minp, int *midp, int *maxp)
{
	int match = 0;
	int max = i->index->n - 1;
	int min = 0;
	int mid = 0;
	while (max >= min)
	{
		mid = min + ((max - min) >> 1);
		sdnode *n = sd_nodeindexv(i->index, mid);
		switch (sd_nodecmp(n, i->key, i->keysize)) {
		case -1: min = mid + 1;
			continue;
		case  1: max = mid - 1;
			continue;
		default: match = 1;
			goto done;
		}
	}
done:
	*minp = min;
	*midp = mid;
	*maxp = max;
	return match;
}

static int
sd_indexiter_route(sdindexiter *i, int *match)
{
	sdnode *n = NULL;
	if (srunlikely(i->index->n == 1)) {
		*match = 1;
		return 0;
	}
	int min, max;
	int mid;
	if (sd_indexiter_seek(i, &min, &mid, &max)) {
		*match = 1;
		return mid;
	}
	assert(min >= 0);
	*match = 0;
	if (min == 0)
		return 0;
	if (srunlikely(min >= i->index->n))
		return i->index->n - 1;
	n = sd_nodeindexv(i->index, min);
	ssref *minp = ss_indexmin(&n->index);
	srcomparator *cmp = &n->c->sdb->cmp;
	int l = sr_compare(cmp, ss_refmin(minp), minp->sizemin,
	                   i->key, i->keysize);
	if (l == 1)
		min--;
	return min;
}

static inline void
sd_indexiter_bkw(sdindexiter *i)
{
	if (srunlikely(i->pos < 0))
		i->pos = 0;
	i->v = sd_nodeindexv(i->index, i->pos);
}

static inline void
sd_indexiter_fwd(sdindexiter *i)
{
	if (srunlikely(i->pos >= i->index->n))
		i->pos = i->index->n - 1;
	i->v = sd_nodeindexv(i->index, i->pos);
}

static int
sd_indexiter_open(sriter *i, va_list args)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	sdindex *index = va_arg(args, sdindex*);
	ii->index   = &index->i;
	ii->order   = va_arg(args, srorder);
	ii->key     = va_arg(args, void*);
	ii->keysize = va_arg(args, int);
	ii->v       = NULL;
	if (srunlikely(ii->index->n == 0)) {
		ii->pos = -1;
		return 0;
	}
	int match = 0;
	int matchpos = -1;
	if (ii->key == NULL) {
		switch (ii->order) {
		case SR_LT:
		case SR_LTE:
			ii->pos = ii->index->n - 1;
			sd_indexiter_bkw(ii);
			break;
		case SR_GT:
		case SR_GTE:
			ii->pos = 0;
			sd_indexiter_fwd(ii);
			break;
		default: assert(0);
		}
		return 0;
	}
	ii->pos = sd_indexiter_route(ii, &match);
	if (match)
		matchpos = ii->pos;

	switch (ii->order) {
	case SR_LT: {
		sdnode *n = sd_nodeindexv(ii->index, ii->pos);
		if (srunlikely(n->index.n > 0)) {
			ssref *p = ss_indexmin(&n->index);
			int l = sr_compare(&i->c->sdb->cmp, ss_refmin(p),
			                   p->sizemin,
			                   ii->key, ii->keysize);
			if (srunlikely(l == 0))
				ii->pos--;
		}
	}
	case SR_LTE:
	case SR_EQ:
		sd_indexiter_bkw(ii);
		break;
	case SR_GT: {
		sdnode *n = sd_nodeindexv(ii->index, ii->pos);
		if (srunlikely(n->index.n > 0)) {
			ssref *p = ss_indexmax(&n->index);
			int r = sr_compare(&i->c->sdb->cmp, ss_refmax(p),
			                   p->sizemax,
			                   ii->key, ii->keysize);
			if (srunlikely(r == 0))
				ii->pos++;
		}
	}
	case SR_GTE:
		sd_indexiter_fwd(ii);
		break;
	case SR_UPDATE:
		/* do not check minlsn for update */
		ii->v = sd_nodeindexv(ii->index, ii->pos);
		break;
	default: assert(0);
	}
	if (match && matchpos == ii->pos)
		return 1;
	return 0;
}

static void
sd_indexiter_close(sriter *i srunused)
{ }

static int
sd_indexiter_has(sriter *i)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	return ii->v != NULL;
}

static void*
sd_indexiter_of(sriter *i)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	return ii->v;
}

static void
sd_indexiter_next(sriter *i)
{
	sdindexiter *ii = (sdindexiter*)i->priv;
	(void)ii;
	assert(0);
}

sriterif sd_indexiter =
{
	.init  = sd_indexiter_init,
	.open  = sd_indexiter_open,
	.close = sd_indexiter_close,
	.has   = sd_indexiter_has,
	.of    = sd_indexiter_of,
	.next  = sd_indexiter_next
};
