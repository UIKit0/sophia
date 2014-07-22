
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

typedef struct ssindexiter ssindexiter;

struct ssindexiter {
	ssindex *index;
	ssref *v;
	int pos;
	srorder order;
	void *key;
	int keysize;
	uint64_t lsvn;
} srpacked;

static void
ss_indexiter_init(sriter *i)
{
	assert(sizeof(ssindexiter) <= sizeof(i->priv));
	ssindexiter *ii = (ssindexiter*)i->priv;
	memset(ii, 0, sizeof(*ii));
}

static inline int
ss_indexiter_seek(sriter *i, void *key, int size, int *minp, int *midp, int *maxp)
{
	ssindexiter *ii = (ssindexiter*)i->priv;
	int match = 0;
	int min = 0;
	int max = ii->index->n - 1;
	int mid = 0;
	while (max >= min)
	{
		mid = min + ((max - min) >> 1);
		ssref *page = ss_indexpage(ii->index, mid);
		int rc = ss_refcmp(page, key, size, i->c->sdb->cmp);
		switch (rc) {
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
ss_indexiter_route(sriter *i, int *match)
{
	ssindexiter *ii = (ssindexiter*)i->priv;
	if (srunlikely(ii->index->n == 1)) {
		*match = 1;
		return 0;
	}
	int mid, min, max;
	int rc = ss_indexiter_seek(i, ii->key, ii->keysize, &min, &mid, &max);
	if (srlikely(rc)) {
		*match = 1;
		return mid;
	}
	*match = 0;
	if (srunlikely(min >= (int)ii->index->n))
		min = ii->index->n - 1;
	return min;
}

static inline void
ss_indexiter_bkw(ssindexiter *i)
{
	int pos = i->pos;
	while (pos >= 0) {
		ssref *v = ss_indexpage(i->index, pos);
		if (v->lsnmin <= i->lsvn)
			break;
		pos--;
	}
	if (srunlikely(pos < 0)) {
		i->pos = -1;
		i->v = NULL;
		return;
	}
	i->pos = pos;
	i->v = ss_indexpage(i->index, pos);
}

static inline void
ss_indexiter_fwd(ssindexiter *i)
{
	(void)i;
	int pos = i->pos;
	while (pos < i->index->n) {
		ssref *v = ss_indexpage(i->index, pos);
		if (v->lsnmin <= i->lsvn)
			break;
		pos++;
	}
	if (srunlikely(pos >= i->index->n)) {
		i->pos = -1;
		i->v = NULL;
		return;
	}
	i->pos = pos;
	i->v = ss_indexpage(i->index, pos);
}

static int
ss_indexiter_open(sriter *i, va_list args)
{
	ssindexiter *ii = (ssindexiter*)i->priv;
	ii->index   = va_arg(args, ssindex*);
	ii->order   = va_arg(args, srorder);
	ii->key     = va_arg(args, void*);
	ii->keysize = va_arg(args, int);
	ii->lsvn    = va_arg(args, uint64_t);
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
			ss_indexiter_bkw(ii);
			break;
		case SR_GT:
		case SR_GTE:
			ii->pos = 0;
			ss_indexiter_fwd(ii);
			break;
		default: assert(0);
		}
		return 0;
	}

	ii->pos = ss_indexiter_route(i, &match);
	if (match)
		matchpos = ii->pos;
	switch (ii->order) {
	case SR_LT: {
		ssref *p = ss_indexpage(ii->index, ii->pos);
		int l = sr_compare(i->c->sdb->cmp, ss_refmin(p), p->sizemin,
		                   ii->key, ii->keysize);
		if (srunlikely(l == 0))
			ii->pos--;
	}
	case SR_LTE:
	case SR_EQ:
		ss_indexiter_bkw(ii);
		break;
	case SR_GT: {
		ssref *p = ss_indexpage(ii->index, ii->pos);
		int r = sr_compare(i->c->sdb->cmp, ss_refmax(p), p->sizemax,
		                   ii->key, ii->keysize);
		if (srunlikely(r == 0))
			ii->pos++;
	}
	case SR_GTE:
		ss_indexiter_fwd(ii);
		break;
	case SR_UPDATE:
		/* do not check minlsn for update */
		ii->v = ss_indexpage(ii->index, ii->pos);
		break;
	default: assert(0);
	}
	if (match && matchpos == ii->pos)
		return 1;
	return 0;
}

static void
ss_indexiter_close(sriter *i srunused)
{ }

static int
ss_indexiter_has(sriter *i)
{
	ssindexiter *ii = (ssindexiter*)i->priv;
	return ii->v != NULL;
}

static void*
ss_indexiter_of(sriter *i)
{
	ssindexiter *ii = (ssindexiter*)i->priv;
	return ii->v;
}

static void
ss_indexiter_next(sriter *i)
{
	ssindexiter *ii = (ssindexiter*)i->priv;
	switch (ii->order) {
	case SR_LT:
	case SR_LTE:
		ii->pos--;
		ss_indexiter_fwd(ii);
		break;
	case SR_GT:
	case SR_GTE:
		ii->pos++;
		ss_indexiter_bkw(ii);
		break;
	default: assert(0);
	}
	if (srunlikely(ii->pos < 0))
		ii->v = NULL;
	else
	if (srunlikely(ii->pos >= (int)ii->index->n))
		ii->v = NULL;
	else
		ii->v = ss_indexpage(ii->index, ii->pos);
}

sriterif ss_indexiter =
{
	.init  = ss_indexiter_init,
	.open  = ss_indexiter_open,
	.close = ss_indexiter_close,
	.has   = ss_indexiter_has,
	.of    = ss_indexiter_of,
	.next  = ss_indexiter_next
};

typedef struct ssindexiterraw ssindexiterraw;

struct ssindexiterraw {
	ssindex *index;
	ssref *v;
	int pos;
} srpacked;

static void
ss_indexiterraw_init(sriter *i)
{
	assert(sizeof(ssindexiterraw) <= sizeof(i->priv));

	ssindexiterraw *ii = (ssindexiterraw*)i->priv;
	memset(ii, 0, sizeof(*ii));
}

static int
ss_indexiterraw_open(sriter *i, va_list args)
{
	ssindexiterraw *ii = (ssindexiterraw*)i->priv;
	ii->index = va_arg(args, ssindex*);
	ii->pos = 0;
	ii->v   = NULL;
	if (srunlikely(ii->index->n == 0))
		return 0;
	ii->v   = ss_indexpage(ii->index, ii->pos);
	return 0;
}

static void
ss_indexiterraw_close(sriter *i srunused)
{ }

static int
ss_indexiterraw_has(sriter *i)
{
	ssindexiterraw *ii = (ssindexiterraw*)i->priv;
	return ii->v != NULL;
}

static void*
ss_indexiterraw_of(sriter *i)
{
	ssindexiterraw *ii = (ssindexiterraw*)i->priv;
	return ii->v;
}

static void
ss_indexiterraw_next(sriter *i)
{
	ssindexiterraw *ii = (ssindexiterraw*)i->priv;
	ii->pos++;
	if (srunlikely(ii->pos >= (int)ii->index->n))
		ii->v = NULL;
	else
		ii->v = ss_indexpage(ii->index, ii->pos);
}

sriterif ss_indexiterraw =
{
	.init  = ss_indexiterraw_init,
	.open  = ss_indexiterraw_open,
	.close = ss_indexiterraw_close,
	.has   = ss_indexiterraw_has,
	.of    = ss_indexiterraw_of,
	.next  = ss_indexiterraw_next
};
