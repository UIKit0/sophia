
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

int sm_cmp(smtx *t, uint32_t id)
{
	if (srunlikely(t->id == id))
		return 0;
	return (t->id > id) ? 1: -1;
}

int sm_init(sm *m, src *c)
{
	m->c = c;
	int rc = sr_tinit(&m->tx, c->a, 256, NULL);
	if (srunlikely(rc == -1))
		return -1;
	rc = sr_iinit(&m->i, c->a, 256, c->sdb->cmp);
	if (srunlikely(rc == -1)) {
		sr_tfree(&m->tx);
		return -1;
	}
	sr_spinlockinit(&m->locktx);
	sr_spinlockinit(&m->locki);
	return 0;
}

int sm_free(sm *m)
{
	/* rollback active transactions */
	sr_tfree(&m->tx);
	sr_ifree(&m->i);
	sr_spinlockfree(&m->locktx);
	sr_spinlockinit(&m->locki);
	return 0;
}

uint64_t sm_lsvn(sm *m)
{
	sr_spinlock(&m->locktx);
	uint64_t lsvn;
	if (m->tx.n) {
		smtx *min = sr_tmin(&m->tx);
		lsvn = min->lsvn;
	} else {
		lsvn = sr_seq(m->c->seq, SR_LSN) - 1;
	}
	sr_spinunlock(&m->locktx);
	return lsvn;
}

int sm_begin(sm *m, smtx *t)
{
	t->s = SMREADY; 
	t->m = m;
	sr_seqlock(m->c->seq);
	t->id   = sr_seqdo(m->c->seq, SR_TSNNEXT);
	t->lsvn = sr_seqdo(m->c->seq, SR_LSN) - 1;
	sr_sequnlock(m->c->seq);
	sv_loginit(&t->log);

	sr_spinlock(&m->locktx);
	sr_ti pos;
	sr_topen(&pos, &m->tx);
	int rc = sr_tprepare(&pos, &t->id, sizeof(t->id));
	switch (rc) {
	case  0:
		sr_tvalset(&pos, t);
		break;
	case  1:
		assert(0);
		break;
	case -1:
		sr_spinunlock(&m->locktx);
		return -1;
	}
	sr_spinunlock(&m->locktx);
	return 0;
}

int sm_end(smtx *t)
{
	sm *m = t->m;
	assert(t->s != SMUNDEF);
	sr_spinlock(&m->locktx);
	sr_ti pos;
	sr_topen(&pos, &m->tx);
	sr_tgeti(&pos, &t->id, sizeof(t->id));
	assert(sr_tval(&pos) == t);
	sr_tdel(&pos);
	sr_spinunlock(&m->locktx);
	t->s = SMUNDEF;
	sv_logfree(&t->log, m->c->a);
	return 0;
}

smstate
sm_prepare(smtx *t, smpreparef prepare, void *arg)
{
	sm *m = t->m;
	sriter i;
	sr_iterinit(&i, &sr_bufiter, m->c);
	sr_iteropen(&i, &t->log.buf, sizeof(sv));
	smstate s;
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *vp = sr_iterof(&i);
		smv *v = vp->v;
		/* cancelled by a concurrent commited
		 * transaction */
		if (v->flags & SVABORT)
			return SMROLLBACK;
		/* concurrent update in progress */
		if (v->prev != NULL)
			return SMWAIT;
		/* check that new key has not been committed by
		 * a concurrent transaction */
		s = prepare(t, vp, arg);
		if (srunlikely(s != SMPREPARE))
			return s;
	}
	s = SMPREPARE;
	t->s = s;
	return s;
}

smstate
sm_commit(smtx *t)
{
	assert(t->s == SMPREPARE);
	sm *m = t->m;
	sriter i;
	sr_iterinit(&i, &sr_bufiter, m->c);
	sr_iteropen(&i, &t->log.buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *vp = sr_iterof(&i);
		smv *v = vp->v;
		/* mark waiters as aborted */
		sm_vabortwaiters(v);
		/* unlink version */
		sm_vunlink(v);
		/* remove from concurrent index and replace
		 * head with a first waiter */
		sr_ii pos;
		sr_iopen(&pos, &m->i);
		int rc = sr_iprepare(&pos, v->key, v->keysize);
		(void)rc;
		assert(rc == 1);
		assert(sr_ival(&pos) == v);
		if (v->next == NULL)
			sr_idel(&pos);
		else
			sr_ivalset(&pos, v->next);
		v->next = NULL;
		v->prev = NULL;
	}
	t->s = SMCOMMIT;
	return SMCOMMIT;
}

smstate
sm_rollback(smtx *t)
{
	sm *m = t->m;
	sriter i;
	sr_iterinit(&i, &sr_bufiter, m->c);
	sr_iteropen(&i, &t->log.buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *vp = sr_iterof(&i);
		smv *v = vp->v;
		/* unlink version */
		sm_vunlink(v);
		/* remove from index and replace head with
		 * a first waiter */
		sr_ii pos;
		sr_iopen(&pos, &m->i);
		int rc = sr_iprepare(&pos, v->key, v->keysize);
		(void)rc;
		assert(rc == 1);
		smv *head = sr_ival(&pos);
		assert(head != NULL);
		if (srlikely(head == v)) {
			if (srlikely(v->next == NULL))
				sr_idel(&pos);
			else
				sr_ivalset(&pos, v->next);
		}
		sm_vunref(m->c->a, v);
	}
	t->s = SMROLLBACK;
	return SMROLLBACK;
}

int sm_set(smtx *t, sv *vp)
{
	/* allocate new version */
	sm *m = t->m;
	smv *v = sm_valloc(m->c->a, vp);
	if (srunlikely(v == NULL))
		return -1;
	sv vv;
	svinit(&vv, &sm_vif, v, NULL);
	v->id.tx.id = t->id;
	v->id.tx.lo = 0;

	/* try to update concurrent index */
	sr_ii pos;
	sr_iopen(&pos, &m->i);
	int rc = sr_iprepare(&pos, v->key, v->keysize);
	switch (rc) {
	case  0:
		/* unique */
		v->id.tx.lo = sv_logn(&t->log);
		rc = sv_logadd(&t->log, m->c->a, &vv);
		if (srunlikely(rc == -1)) {
			sr_idel(&pos);
			return -1;
		}
		sr_ivalset(&pos, v);
		return 0;
	case -1:
		sm_vunref(m->c->a, v);
		return -1;
	}

	smv *head = sr_ival(&pos);

	/* match previous update made by current
	 * transaction */
	smv *own = sm_vmatch(head, t->id);
	if (srunlikely(own)) {
		/* replace old object with the new one */
		v->id.tx.lo = own->id.tx.lo;
		sm_vreplace(own, v);
		if (srlikely(head == own))
			sr_ivalset(&pos, v);
		/* update log */
		sv_logreplace(&t->log, v->id.tx.lo, &vv);
		sm_vunref(m->c->a, own);
		return 0;
	}

	/* update log */
	rc = sv_logadd(&t->log, m->c->a, &vv);
	if (srunlikely(rc == -1)) {
		sm_vunref(m->c->a, v);
		return -1;
	}

	/* add version */
	sm_vlink(head, v);
	return 0;
}

int sm_match(smtx *t, sv *key, smv **result)
{
	sm *m = t->m;
	smv *head = sr_iget(&m->i, svkey(key), svkeysize(key));
	if (head == NULL)
		return 0;
	*result = sm_vmatch(head, t->id);
	return *result != NULL;
}

int sm_get(smtx *t, sv *key, void **value, int *valuesize)
{
	sm *m = t->m;
	smv *v;
	int rc = sm_match(t, key, &v);
	if (srunlikely(rc == 0))
		return 0;
	if (srunlikely((v->flags & SVDELETE) > 0))
		return 2;
	*valuesize = v->valuesize;
	*value = sr_malloc(m->c->a, *valuesize);
	if (srunlikely(*value == NULL))
		return -1;
	sv vv;
	svinit(&vv, &sm_vif, v, NULL);
	svvaluecopy(&vv, *value);
	return 1;
}

smstate sm_setss(sm *m, sv *v, svlog *l)
{
	sr_seq(m->c->seq, SR_TSNNEXT);
	smv *head = sr_iget(&m->i, svkey(v), svkeysize(v));
	if (head)
		return SMWAIT;
	smv *mv = sm_valloc(m->c->a, v);
	if (srunlikely(mv == NULL))
		return SMROLLBACK;
	sv logv;
	svinit(&logv, &sm_vif, mv, NULL);
	int rc = sv_logadd(l, m->c->a, &logv);
	if (srunlikely(rc == -1)) {
		sr_free(m->c->a, mv);
		return SMROLLBACK;
	}
	return SMCOMMIT;
}
