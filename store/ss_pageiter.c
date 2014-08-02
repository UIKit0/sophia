
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

typedef struct sspageiter sspageiter;

struct sspageiter {
	sspage *page;
	int pos;
	ssv *v;
	sv current;
	srorder order;
	void *key;
	int keysize;
	uint64_t lsvn;
} srpacked;

static void
ss_pageiter_init(sriter *i)
{
	assert(sizeof(sspageiter) <= sizeof(i->priv));

	sspageiter *pi = (sspageiter*)i->priv;
	memset(pi, 0, sizeof(*pi));
}

static inline void
ss_pageiter_end(sspageiter *i)
{
	i->pos = i->page->h->count;
	i->v   = NULL;
}

static inline int
ss_pageiter_search(sriter *i, int search_min)
{
	sspageiter *pi = (sspageiter*)i->priv;
	srcomparator *cmp = &i->c->sdb->cmp;
	int min = 0;
	int mid = 0;
	int max = pi->page->h->count - 1;
	while (max >= min)
	{
		mid = min + (max - min) / 2;
		ssv *v = ss_pagev(pi->page, mid);
		int rc = sr_compare(cmp, v->key, v->keysize, pi->key, pi->keysize);
		switch (rc) {
		case -1: min = mid + 1;
			continue;
		case  1: max = mid - 1;
			continue;
		default: return mid;
		}
	}
	return (search_min) ? min : max;
}

static inline void
ss_pageiter_lv(sspageiter *i, int pos)
{
	/* lower-visible bound */

	/* find visible max: any first key which
	 * lsn <= lsvn (max in dup chain) */
	int maxpos = 0;
	ssv *v;
	ssv *max = NULL;
	while (pos >= 0) {
		v = ss_pagev(i->page, pos);
		if (v->lsn <= i->lsvn) {
			maxpos = pos;
			max = v;
		}
		if (! (v->flags & SVDUP)) {
			/* head */
			if (max) {
				i->pos = maxpos;
				i->v = max;
				return;
			}
		}
		pos--;
	}
	ss_pageiter_end(i);
}

static inline void
ss_pageiter_gv(sspageiter *i, int pos)
{
	/* greater-visible bound */

	/* find visible max: any first key which
	 * lsn <= lsvn (max in dup chain) */
	while (pos < i->page->h->count ) {
		ssv *v = ss_pagev(i->page, pos);
		if (v->lsn <= i->lsvn) {
			i->pos = pos;
			i->v = v;
			return;
		}
		pos++;
	}
	ss_pageiter_end(i);
}

static inline void
ss_pageiter_lland(sspageiter *i, int pos)
{
	/* reposition to a visible duplicate */
	i->pos = pos;
	i->v = ss_pagev(i->page, i->pos);
	if (i->v->lsn == i->lsvn)
		return;
	if (i->v->lsn > i->lsvn) {
		/* search max < i->lsvn */
		pos++;
		while (pos < i->page->h->count)
		{
			ssv *v = ss_pagev(i->page, pos);
			if (! (v->flags & SVDUP))
				break;
			if (v->lsn <= i->lsvn) {
				i->pos = pos;
				i->v = v;
				return;
			}
			pos++;
		}
	}
	ss_pageiter_lv(i, i->pos);
}

static inline void
ss_pageiter_gland(sspageiter *i, int pos)
{
	/* reposition to a visible duplicate */
	i->pos = pos;
	i->v = ss_pagev(i->page, i->pos);
	if (i->v->lsn == i->lsvn)
		return;

	if (i->v->lsn > i->lsvn) {
		/* search max < i->lsvn */
		pos++;
		ss_pageiter_gv(i, pos);
		return;
	}

	/* i->v->lsn < i->lsvn */
	if (! (i->v->flags & SVDUP))
		return;
	int maxpos = pos;
	ssv *max = ss_pagev(i->page, i->pos);
	pos--;
	while (pos >= 0) {
		ssv *v = ss_pagev(i->page, pos);
		if (v->lsn <= i->lsvn) {
			maxpos = pos;
			max = v;
		}
		if (! (v->flags & SVDUP))
			break;
		pos--;
	}
	i->pos = maxpos;
	i->v = max;
}

static void
ss_pageiter_bkw(sspageiter *i)
{
	/* skip to previous visible key */
	int pos = i->pos;
	ssv *v = ss_pagev(i->page, pos);
	if (v->flags & SVDUP) {
		/* skip duplicates */
		pos--;
		while (pos >= 0) {
			v = ss_pagev(i->page, pos);
			if (! (v->flags & SVDUP))
				break;
			pos--;
		}
		if (srunlikely(pos < 0)) {
			ss_pageiter_end(i);
			return;
		}
	}
	assert(! (v->flags & SVDUP));
	pos--;

	ss_pageiter_lv(i, pos);
}

static void
ss_pageiter_fwd(sspageiter *i)
{
	/* skip to next visible key */
	int pos = i->pos + 1;
	while (pos < i->page->h->count)
	{
		ssv *v = ss_pagev(i->page, pos);
		if (! (v->flags & SVDUP))
			break;
		pos++;
	}
	if (srunlikely(pos == i->page->h->count)) {
		ss_pageiter_end(i);
		return;
	}
	ssv *match = NULL;
	while (pos < i->page->h->count)
	{
		ssv *v = ss_pagev(i->page, pos);
		if (v->lsn <= i->lsvn) {
			match = v;
			break;
		}
		pos++;
	}
	if (srunlikely(pos == i->page->h->count)) {
		ss_pageiter_end(i);
		return;
	}
	assert(match != NULL);
	i->pos = pos;
	i->v = match;
}

static inline int
ss_pageiter_lt(sriter *i, int e)
{
	sspageiter *pi = (sspageiter*)i->priv;
	if (srunlikely(pi->page->h->count == 0)) {
		ss_pageiter_end(pi);
		return 0;
	}
	if (pi->key == NULL) {
		ss_pageiter_lv(pi, pi->page->h->count - 1);
		return 0;
	}
	int pos = ss_pageiter_search(i, 1);
	if (srunlikely(pos >= pi->page->h->count))
		pos = pi->page->h->count - 1;
	ss_pageiter_lland(pi, pos);
	if (pi->v == NULL)
		return 0;
	int rc = sr_compare(&i->c->sdb->cmp, pi->v->key, pi->v->keysize,
	                    pi->key, pi->keysize);
	int match = rc == 0;
	switch (rc) {
		case  0:
			if (! e)
				ss_pageiter_bkw(pi);
			break;
		case  1:
			ss_pageiter_bkw(pi);
			break;
	}
	return match;
}

static inline int
ss_pageiter_gt(sriter *i, int e)
{
	sspageiter *pi = (sspageiter*)i->priv;
	if (srunlikely(pi->page->h->count == 0)) {
		ss_pageiter_end(pi);
		return 0;
	}
	if (pi->key == NULL) {
		ss_pageiter_gv(pi, 0);
		return 0;
	}
	int pos = ss_pageiter_search(i, 1);
	if (srunlikely(pos >= pi->page->h->count))
		pos = pi->page->h->count - 1;
	ss_pageiter_gland(pi, pos);
	if (pi->v == NULL)
		return 0;
	int rc = sr_compare(&i->c->sdb->cmp, pi->v->key, pi->v->keysize,
	                    pi->key, pi->keysize);
	int match = rc == 0;
	switch (rc) {
		case  0:
			if (! e)
				ss_pageiter_fwd(pi);
			break;
		case -1:
			ss_pageiter_fwd(pi);
			break;
	}
	return match;
}

static int
ss_pageiter_open(sriter *i, va_list args)
{
	sspageiter *pi = (sspageiter*)i->priv;
	pi->page    = va_arg(args, sspage*);
	pi->order   = va_arg(args, srorder);
	pi->key     = va_arg(args, void*);
	pi->keysize = va_arg(args, int);
	pi->lsvn    = va_arg(args, uint64_t);
	assert(pi->page->h->flags == 0);
	if (srunlikely(pi->page->h->lsnmin > pi->lsvn &&
	               pi->order != SR_UPDATE))
		return 0;
	int match;
	switch (pi->order) {
	case SR_LT:  return ss_pageiter_lt(i, 0);
	case SR_LTE: return ss_pageiter_lt(i, 1);
	case SR_GT:  return ss_pageiter_gt(i, 0);
	case SR_GTE: return ss_pageiter_gt(i, 1);
	case SR_EQ:  return ss_pageiter_lt(i, 1);
	case SR_UPDATE: {
		uint64_t lsvn = pi->lsvn;
		pi->lsvn = (uint64_t)-1;
		match = ss_pageiter_lt(i, 1);
		if (match == 0)
			return 0;
		return pi->v->lsn > lsvn;
	}
	default: assert(0);
	}
	return 0;
}

static void
ss_pageiter_close(sriter *i srunused)
{ }

static int
ss_pageiter_has(sriter *i)
{
	sspageiter *pi = (sspageiter*)i->priv;
	return pi->v != NULL;
}

static void*
ss_pageiter_of(sriter *i)
{
	sspageiter *pi = (sspageiter*)i->priv;
	if (srunlikely(pi->v == NULL))
		return NULL;
	svinit(&pi->current, &ss_vif, pi->v, pi->page);
	return &pi->current;
}

static void
ss_pageiter_next(sriter *i)
{
	sspageiter *pi = (sspageiter*)i->priv;
	switch (pi->order) {
	case SR_LT:
	case SR_LTE: ss_pageiter_bkw(pi);
		break;
	case SR_GT:
	case SR_GTE: ss_pageiter_fwd(pi);
		break;
	default: assert(0);
	}
}

sriterif ss_pageiter =
{
	.init    = ss_pageiter_init,
	.open    = ss_pageiter_open,
	.close   = ss_pageiter_close,
	.has     = ss_pageiter_has,
	.of      = ss_pageiter_of,
	.next    = ss_pageiter_next
};

typedef struct sspageiterraw sspageiterraw;

struct sspageiterraw {
	sspage *page;
	int pos;
	ssv *v;
	sv current;
} srpacked;

static void
ss_pageiterraw_init(sriter *i)
{
	assert(sizeof(sspageiterraw) <= sizeof(i->priv));

	sspageiterraw *pi = (sspageiterraw*)i->priv;
	memset(pi, 0, sizeof(*pi));
}

static int
ss_pageiterraw_open(sriter *i, va_list args)
{
	sspageiterraw *pi = (sspageiterraw*)i->priv;
	sspage *p = va_arg(args, sspage*);
	pi->page = p;
	if (srunlikely(p->h->count == 0)) {
		pi->pos = 1;
		pi->v = NULL;
		return 0;
	}
	pi->pos = 0;
	pi->v = ss_pagev(p, 0);
	return 0;
}

static void
ss_pageiterraw_close(sriter *i srunused)
{ }

static int
ss_pageiterraw_has(sriter *i)
{
	sspageiterraw *pi = (sspageiterraw*)i->priv;
	return pi->v != NULL;
}

static void*
ss_pageiterraw_of(sriter *i)
{
	sspageiterraw *pi = (sspageiterraw*)i->priv;
	if (srunlikely(pi->v == NULL))
		return NULL;
	svinit(&pi->current, &ss_vif, pi->v, pi->page);
	return &pi->current;
}

static void
ss_pageiterraw_next(sriter *i)
{
	sspageiterraw *pi = (sspageiterraw*)i->priv;
	pi->pos++;
	if (srlikely(pi->pos < pi->page->h->count))
		pi->v = ss_pagev(pi->page, pi->pos);
	else
		pi->v = NULL;
}

sriterif ss_pageiterraw =
{
	.init  = ss_pageiterraw_init,
	.open  = ss_pageiterraw_open,
	.close = ss_pageiterraw_close,
	.has   = ss_pageiterraw_has,
	.of    = ss_pageiterraw_of,
	.next  = ss_pageiterraw_next,
};
