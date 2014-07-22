
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

typedef struct smiter smiter;

struct smiter {
	sm *m;
	sr_ii i;
	sv current;
	smv *v;
	srorder order;
	void *key;
	int keysize;
	uint32_t id;
} srpacked;

static void
sm_iterinit(sriter *i)
{
	assert(sizeof(smiter) <= sizeof(i->priv));

	smiter *ii = (smiter*)i->priv;
	memset(ii, 0, sizeof(*ii));
}

static inline smv*
sm_iterfwd(smiter *i)
{
	for (;;) {
		smv *v = sr_ival(&i->i);
		if (srunlikely(v == NULL))
			return NULL;
		v = sm_vmatch(v, i->id);
		if (srlikely(v))
			return v;
		sr_inext(&i->i);
	}
	return NULL; 
}

static inline smv*
sm_iterbkw(smiter *i)
{
	for (;;) {
		smv *v = sr_ival(&i->i);
		if (srunlikely(v == NULL))
			return NULL;
		v = sm_vland(v, i->id);
		if (srlikely(v))
			return v;
		sr_iprev(&i->i);
	}
	return NULL; 
}

static int
sm_iteropen(sriter *i, va_list args)
{
	smiter *ii = (smiter*)i->priv;
	ii->m       = va_arg(args, sm*);
	ii->order   = va_arg(args, srorder);
	ii->key     = va_arg(args, void*);
	ii->keysize = va_arg(args, int);
	ii->id      = va_arg(args, uint32_t);
	sr_iopen(&ii->i, &ii->m->i);
	int eq = 0;
	switch (ii->order) {
	case SR_LT:
	case SR_LTE:
		if (srunlikely(ii->key == NULL)) {
			sr_ilast(&ii->i);
			ii->v = sm_iterbkw(ii);
			break;
		}
		eq = sr_ilte(&ii->i, ii->key, ii->keysize);
		if (eq && ii->order == SR_LT)
			sr_iprev(&ii->i);
		ii->v = sm_iterbkw(ii);
		break;
	case SR_GT:
	case SR_GTE:
		if (srunlikely(ii->key == NULL)) {
			sr_ifirst(&ii->i);
			ii->v = sm_iterfwd(ii);
			break;
		}
		eq = sr_igte(&ii->i, ii->key, ii->keysize);
		if (eq && ii->order == SR_GT)
			sr_inext(&ii->i);
		ii->v = sm_iterfwd(ii);
		break;
	default: assert(0);
	}
	return eq;
}

static void
sm_iterclose(sriter *i srunused)
{}

static int
sm_iterhas(sriter *i)
{
	smiter *ii = (smiter*)i->priv;
	return sr_ihas(&ii->i);
}

static void*
sm_iterof(sriter *i)
{
	smiter *ii = (smiter*)i->priv;
	smv *v = sr_ival(&ii->i);
	if (srunlikely(v == NULL))
		return NULL;
	assert(ii->v != NULL);
	svinit(&ii->current, &sm_vif, ii->v, NULL);
	return &ii->current;
}

static void
sm_iternext(sriter *i)
{
	smiter *ii = (smiter*)i->priv;
	switch (ii->order) {
	case SR_LT:
	case SR_LTE:
		sr_iprev(&ii->i);
		ii->v = sm_iterbkw(ii);
		break;
	case SR_GT:
	case SR_GTE:
		sr_inext(&ii->i);
		ii->v = sm_iterfwd(ii);
		break;
	default: assert(0);
	}
}

sriterif sm_iter =
{
	.init    = sm_iterinit,
	.open    = sm_iteropen,
	.close   = sm_iterclose,
	.has     = sm_iterhas,
	.of      = sm_iterof,
	.next    = sm_iternext
};
