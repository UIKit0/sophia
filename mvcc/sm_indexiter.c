
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>

typedef struct smindexiter smindexiter;

struct smindexiter {
	smindex *index;
	sr_ii i;
	sv current;
	smv *v;
	srorder order;
	void *key;
	int keysize;
	uint64_t lsvn;
} srpacked;

static void
sm_indexiter_init(sriter *i)
{
	assert(sizeof(smindexiter) <= sizeof(i->priv));

	smindexiter *ii = (smindexiter*)i->priv;
	memset(ii, 0, sizeof(*ii));
}

static inline smv*
sm_indexiter_fwd(smindexiter *i)
{
	for (;;) {
		smv *v = sr_ival(&i->i);
		if (srunlikely(v == NULL))
			return NULL;
		v = sm_vland(v, i->lsvn);
		if (srlikely(v))
			return v;
		sr_inext(&i->i);
	}
	return NULL; 
}

static inline smv*
sm_indexiter_bkw(smindexiter *i)
{
	for (;;) {
		smv *v = sr_ival(&i->i);
		if (srunlikely(v == NULL))
			return NULL;
		v = sm_vland(v, i->lsvn);
		if (srlikely(v))
			return v;
		sr_iprev(&i->i);
	}
	return NULL; 
}

static int
sm_indexiter_open(sriter *i, va_list args)
{
	smindexiter *ii = (smindexiter*)i->priv;
	ii->index   = va_arg(args, smindex*);
	ii->order   = va_arg(args, srorder);
	ii->key     = va_arg(args, void*);
	ii->keysize = va_arg(args, int);
	ii->lsvn    = va_arg(args, uint64_t);
	sr_iopen(&ii->i, &ii->index->index);
	int eq = 0;
	switch (ii->order) {
	case SR_LT:
	case SR_LTE:
		if (srunlikely(ii->key == NULL)) {
			sr_ilast(&ii->i);
			ii->v = sm_indexiter_bkw(ii);
			break;
		}
		eq = sr_ilte(&ii->i, ii->key, ii->keysize);
		if (eq && ii->order == SR_LT)
			sr_iprev(&ii->i);
		ii->v = sm_indexiter_bkw(ii);
		break;
	case SR_GT:
	case SR_GTE:
		if (srunlikely(ii->key == NULL)) {
			sr_ifirst(&ii->i);
			ii->v = sm_indexiter_fwd(ii);
			break;
		}
		eq = sr_igte(&ii->i, ii->key, ii->keysize);
		if (eq && ii->order == SR_GT)
			sr_inext(&ii->i);
		ii->v = sm_indexiter_fwd(ii);
		break;
	default: assert(0);
	}
	return eq;
}

static void
sm_indexiter_close(sriter *i srunused)
{}

static int
sm_indexiter_has(sriter *i)
{
	smindexiter *ii = (smindexiter*)i->priv;
	return sr_ihas(&ii->i);
}

static void*
sm_indexiter_of(sriter *i)
{
	smindexiter *ii = (smindexiter*)i->priv;
	smv *v = sr_ival(&ii->i);
	if (srunlikely(v == NULL))
		return NULL;
	assert(ii->v != NULL);
	svinit(&ii->current, &sm_vif, ii->v, NULL);
	return &ii->current;
}

static void
sm_indexiter_next(sriter *i)
{
	smindexiter *ii = (smindexiter*)i->priv;
	switch (ii->order) {
	case SR_LT:
	case SR_LTE:
		sr_iprev(&ii->i);
		ii->v = sm_indexiter_bkw(ii);
		break;
	case SR_GT:
	case SR_GTE:
		sr_inext(&ii->i);
		ii->v = sm_indexiter_fwd(ii);
		break;
	default: assert(0);
	}
}

sriterif sm_indexiter =
{
	.init    = sm_indexiter_init,
	.open    = sm_indexiter_open,
	.close   = sm_indexiter_close,
	.has     = sm_indexiter_has,
	.of      = sm_indexiter_of,
	.next    = sm_indexiter_next
};

typedef struct smindexiterraw smindexiterraw;

struct smindexiterraw {
	smindex *index;
	sr_ii i;
	smv *v;
	sv current;
} srpacked;

static void
sm_indexiterraw_init(sriter *i)
{
	assert(sizeof(smindexiterraw) <= sizeof(i->priv));

	smindexiterraw *ii = (smindexiterraw*)i->priv;
	memset(ii, 0, sizeof(*ii));
}

static int
sm_indexiterraw_open(sriter *i, va_list args)
{
	smindexiterraw *ii = (smindexiterraw*)i->priv;
	ii->index = va_arg(args, smindex*);
	sr_iopen(&ii->i, &ii->index->index);
	ii->v = sr_ival(&ii->i);
	return 0;
}

static void
sm_indexiterraw_close(sriter *i srunused)
{}

static int
sm_indexiterraw_has(sriter *i)
{
	smindexiterraw *ii = (smindexiterraw*)i->priv;
	return sr_ihas(&ii->i);
}

static void*
sm_indexiterraw_of(sriter *i)
{
	smindexiterraw *ii = (smindexiterraw*)i->priv;
	smv *v = sr_ival(&ii->i);
	if (srunlikely(v == NULL))
		return NULL;
	assert(ii->v != NULL);
	svinit(&ii->current, &sm_vif, ii->v, NULL);
	return &ii->current;
}

static void
sm_indexiterraw_next(sriter *i)
{
	smindexiterraw *ii = (smindexiterraw*)i->priv;
	smv *v = ii->v;
	if (srunlikely(ii->v == NULL))
		return;
	v = v->next;
	if (v) {
		ii->v = v;
		return;
	}
	sr_inext(&ii->i);
	ii->v = sr_ival(&ii->i);
}

sriterif sm_indexiterraw =
{
	.init    = sm_indexiterraw_init,
	.open    = sm_indexiterraw_open,
	.close   = sm_indexiterraw_close,
	.has     = sm_indexiterraw_has,
	.of      = sm_indexiterraw_of,
	.next    = sm_indexiterraw_next
};
