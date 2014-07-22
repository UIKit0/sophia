
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
#include <libse.h>
#include <sophia.h>

static int
se_txrollback(seobj *o)
{
	setx *t = (setx*)o;
	sm_lock(&t->db->mvcc);
	sm_rollback(&t->t);
	sm_unlock(&t->db->mvcc);
	sm_end(&t->t);
	se_objindex_unregister(&t->db->e->oi, &t->o);
	sr_free(&t->db->e->a, t);
	return 0;
}

static smstate
se_txprepare(smtx *t, sv *v, void *arg)
{
	setx *te = arg;
	/* no updates been made */
	uint64_t lsn = sr_seq(t->m->c->seq, SR_LSN);
	if (lsn <= t->lsvn)
		return SMPREPARE;
	sdquery q;
	sd_queryopen(&q, &te->db->db, SR_UPDATE, t->lsvn,
	             svkey(v), svkeysize(v));
	sd_querybuf(&q, &te->db->db.rbuf);
	int rc = sd_query(&q);
	sd_queryclose(&q);
	if (srunlikely(rc))
		return SMROLLBACK;
	return SMPREPARE;
}

static int
se_txcommitrecover(seobj *o, va_list args)
{
	setx *t = (setx*)o;
	se *e = t->db->e;
	sedb *db = t->db;

	uint64_t lsn = va_arg(args, uint64_t);
	sl *log = va_arg(args, sl*);

	sm_lock(&db->mvcc);
	smstate s = sm_prepare(&t->t, se_txprepare, t);
	sm_unlock(&db->mvcc);
	if (s == SMWAIT)
		return 2;
	if (s == SMROLLBACK) {
		se_txrollback(&t->o);
		return 1;
	}
	assert(s == SMPREPARE);
	if (srunlikely(! sv_logn(&t->t.log))) {
		sm_lock(&db->mvcc);
		sm_commit(&t->t);
		sm_end(&t->t);
		sm_unlock(&db->mvcc);
		se_objindex_unregister(&db->e->oi, &t->o);
		sr_free(&e->a, t);
		return 0;
	}

	/* recover lsn number */
	sriter i;
	sr_iterinit(&i, &sr_bufiter, db->db.c);
	sr_iteropen(&i, &t->t.log.buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sv *v = sr_iterof(&i);
		svlsnset(v, lsn);
	}

	/* database write */
	sm_lock(&db->mvcc);
	sm_commit(&t->t);

	uint64_t lsvn = sm_lsvn(&db->mvcc);
	sdtx td;
	sd_begin(&db->db, &td, lsvn, &t->t.log, log);
	int rc = sd_write(&td); // prepare
	if (srunlikely(rc == -1)) {
		/*
		sd_rollback(&td);
		se_rollback(t);
		sd_end(&td);
		*/
		sm_unlock(&db->mvcc);
		return -1;
	}

	//sm_commit(&t->t);
	sd_commit(&td);

	/* post commit trigger */
	rc = 0;
	if (t->pc) {
		rc = t->pc(t, t->pcarg);
	}
	sm_unlock(&db->mvcc);
	sm_end(&t->t);
	sd_end(&td);
	if (srunlikely(rc == -1))
		return -1;
	se_objindex_unregister(&e->oi, &t->o);
	sr_free(&e->a, t);
	return 0;
}

static int
se_txcommit(seobj *o, va_list args)
{
	setx *t = (setx*)o;
	sedb *db = t->db;
	se *e = t->db->e;
	if (e->mode == SE_RECOVER)
		return se_txcommitrecover(o, args);

	sm_lock(&db->mvcc);
	smstate s = sm_prepare(&t->t, se_txprepare, t);
	sm_unlock(&db->mvcc);
	if (s == SMWAIT)
		return 2;
	if (s == SMROLLBACK) {
		se_txrollback(&t->o);
		return 1;
	}
	assert(s == SMPREPARE);
	if (srunlikely(! sv_logn(&t->t.log))) {
		sm_lock(&db->mvcc);
		sm_commit(&t->t);
		sm_end(&t->t);
		sm_unlock(&db->mvcc);
		se_objindex_unregister(&e->oi, &t->o);
		sr_free(&e->a, t);
		return 0;
	}

	/* log write */
	sltx tl;
	sl_begin(&e->lp, &tl);
	int rc = sl_write(&tl, &t->t.log, db->db.id);
	if (srunlikely(rc == -1)) {
		sl_rollback(&tl);
		se_txrollback(&t->o);
		return -1;
	}

	/* database write */
	sm_lock(&db->mvcc);
	sm_commit(&t->t);

	uint64_t lsvn = sm_lsvn(&db->mvcc);
	sdtx td;
	sd_begin(&db->db, &td, lsvn, &t->t.log, tl.l);
	rc = sd_write(&td); // prepare
	if (srunlikely(rc == -1)) {
		/*
		sd_rollback(&td);
		sl_rollback(&tl);
		se_rollback(t);
		sd_end(&td);
		*/
		sm_unlock(&db->mvcc);
		return -1;
	}

	//sm_commit(&t->t);
	sd_commit(&td);
	sl_commit(&tl);

	/* post commit trigger */
	rc = 0;
	if (t->pc) {
		rc = t->pc(t, t->pcarg);
	}
	sm_unlock(&db->mvcc);
	sm_end(&t->t);
	sd_end(&td);
	if (srunlikely(rc == -1))
		return -1;
	se_objindex_unregister(&e->oi, &t->o);
	sr_free(&e->a, t);

	if (srlikely(e->conf.scheduler == 2)) {
		seplan plan;
		se_planinit(&plan, e, SE_ALL);
		rc = se_schedule(&plan);
	}
	return rc;
}

static inline void
se_txssrollback(sedb *db, svlog *log)
{
	sriter i;
	sr_iterinit(&i, &sr_bufiter, &db->c);
	sr_iteropen(&i, &log->buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *vp = sr_iterof(&i);
		smv *v = vp->v;
		sm_vunref(db->c.a, v);
	}
	sv_logfree(log, db->c.a);
}

int se_txssset(sedb *db, uint8_t flags, va_list args)
{
	svlocal l;
	l.lsn         = 0,
	l.flags       = flags;
	l.key         = va_arg(args, void*);
	l.keysize     = va_arg(args, size_t);
	l.valueoffset = 0;
	if (flags & SVSET) {
		l.value     = va_arg(args, void*);
		l.valuesize = va_arg(args, size_t);
	} else {
		assert((flags & SVDELETE) > 0);
		l.value     = NULL;
		l.valuesize = 0;
	}
	sv v;
	svinit(&v, &sv_localif, &l, NULL);
	svlog log;
	sv_loginit(&log);

	sm_lock(&db->mvcc);
	smstate s = sm_setss(&db->mvcc, &v, &log);
	if (s == SMWAIT) {
		sm_unlock(&db->mvcc);
		return 2;
	}
	if (s == SMROLLBACK) {
		sm_unlock(&db->mvcc);
		return 1;
	}

	/* log write */
	sltx tl;
	sl_begin(&db->e->lp, &tl);
	int rc = sl_write(&tl, &log, db->db.id);
	if (srunlikely(rc == -1)) {
		sl_rollback(&tl);
		sm_unlock(&db->mvcc);
		se_txssrollback(db, &log);
		return -1;
	}

	uint64_t lsvn = sm_lsvn(&db->mvcc);
	sdtx td;
	sd_begin(&db->db, &td, lsvn, &log, tl.l);
	rc = sd_write(&td); // prepare
	if (srunlikely(rc == -1)) {
		/*
		sd_rollback(&td);
		sd_end(&td);
		*/
		sl_rollback(&tl);
		sm_unlock(&db->mvcc);
		se_txssrollback(db, &log);
		return -1;
	}

	sd_commit(&td);
	sl_commit(&tl);

	sm_unlock(&db->mvcc);
	sd_end(&td);
	sv_logfree(&log, db->c.a);

	if (srlikely(db->e->conf.scheduler == 2)) {
		seplan plan;
		se_planinit(&plan, db->e, SE_ALL);
		rc = se_schedule(&plan);
	}
	return rc;
}

int se_txssget(sedb *db, va_list args)
{
	svlocal l;
	l.lsn         = 0,
	l.flags       = 0,
	l.key         = va_arg(args, void*);
	l.keysize     = va_arg(args, size_t);
	l.value       = NULL;
	l.valuesize   = 0;
	l.valueoffset = 0;
	void **value = va_arg(args, void**);
	int *valuesize = va_arg(args, int*);
	sv v;
	svinit(&v, &sv_localif, &l, NULL);

	sr_seqlock(db->c.seq);
	uint64_t lsvn = sr_seqdo(db->c.seq, SR_LSN) - 1;
	sr_seqdo(db->c.seq, SR_TSNNEXT);
	sr_sequnlock(db->c.seq);

	sdquery q;
	sd_queryopen(&q, &db->db, SR_EQ, lsvn,
	             svkey(&v), svkeysize(&v));
	sd_querybuf(&q, &db->db.rbuf);
	int rc = sd_query(&q);
	if (srunlikely(rc == 0)) {
		sd_queryclose(&q);
		return 0;
	}
	if (srunlikely((svflags(&q.result) & SVDELETE) > 0)) {
		sd_queryclose(&q);
		return 0;
	}
	*valuesize = svvaluesize(&q.result);
	*value = sr_malloc(db->c.a, *valuesize);
	if (srunlikely(*value == NULL))
		rc = -1;
	svvaluecopy(&q.result, *value);
	sd_queryclose(&q);
	return rc;
}

static inline int
se_txdo(seobj *o, uint8_t flags, va_list args)
{
	setx *t = (setx*)o;
	int rc;
	if (srunlikely(t->db->e->mode == SE_RECOVER)) {
		sm_lock(&t->db->mvcc);
		sv *v = va_arg(args, sv*);
		rc = sm_set(&t->t, v);
		sm_unlock(&t->db->mvcc);
		return rc;
	}
	svlocal l;
	l.lsn         = 0,
	l.flags       = flags;
	l.key         = va_arg(args, void*);
	l.keysize     = va_arg(args, size_t);
	l.valueoffset = 0;
	if (flags & SVSET) {
		l.value     = va_arg(args, void*);
		l.valuesize = va_arg(args, size_t);
	} else {
		assert((flags & SVDELETE) > 0);
		l.value     = NULL;
		l.valuesize = 0;
	}
	sv v;
	svinit(&v, &sv_localif, &l, NULL);
	sm_lock(&t->db->mvcc);
	rc = sm_set(&t->t, &v);
	sm_unlock(&t->db->mvcc);
	return rc;
}

static int
se_txset(seobj *o, va_list args)
{
	return se_txdo(o, SVSET, args);
}

static int
se_txdelete(seobj *o, va_list args)
{
	return se_txdo(o, SVDELETE, args);
}

static int
se_txget(seobj *o, va_list args)
{
	setx *t = (setx*)o;
	svlocal l;
	l.lsn         = 0,
	l.flags       = 0,
	l.key         = va_arg(args, void*);
	l.keysize     = va_arg(args, size_t);
	l.value       = NULL;
	l.valuesize   = 0;
	l.valueoffset = 0;
	void **value = va_arg(args, void**);
	int *valuesize = va_arg(args, int*);
	sv v;
	svinit(&v, &sv_localif, &l, NULL);

	sm_lock(&t->db->mvcc);
	int rc = sm_get(&t->t, &v, value, valuesize);
	sm_unlock(&t->db->mvcc);
	switch (rc) {
	case 2: return 0; /* delete */
	case 1: return 1;
	}
	sdquery q;
	sd_queryopen(&q, &t->db->db, SR_EQ, t->t.lsvn,
	             svkey(&v), svkeysize(&v));
	sd_querybuf(&q, &t->db->db.rbuf);
	rc = sd_query(&q);
	if (srunlikely(rc == 0)) {
		sd_queryclose(&q);
		return 0;
	}
	if (srunlikely((svflags(&q.result) & SVDELETE) > 0)) {
		sd_queryclose(&q);
		return 0;
	}
	*valuesize = svvaluesize(&q.result);
	*value = sr_malloc(t->db->c.a, *valuesize);
	if (srunlikely(*value == NULL))
		rc = -1;
	svvaluecopy(&q.result, *value);
	sd_queryclose(&q);
	return rc;
}

static seobjif setxif =
{
	.ctl       = NULL,
	.use       = NULL,
	.open      = NULL,
	.destroy   = se_txrollback,
	.set       = se_txset,
	.del       = se_txdelete,
	.get       = se_txget,
	.begin     = NULL,
	.commit    = se_txcommit,
	.rollback  = se_txrollback,
	.cursor    = NULL,
	.fetch     = NULL,
	.key       = NULL,
	.keysize   = NULL,
	.value     = NULL,
	.valuesize = NULL,
	.backup    = NULL
};

seobj *se_txnew(sedb *db, setxpostcommitf pc, void *pcarg)
{
	se *e = db->e;
	setx *t = sr_malloc(&e->a, sizeof(setx));
	if (srunlikely(t == NULL))
		return NULL;
	se_objinit(&t->o, SEOTX, &setxif);
	t->pc = pc;
	t->pcarg = pcarg;
	t->db = db;
	int rc = sm_begin(&db->mvcc, &t->t);
	if (srunlikely(rc == -1)) {
		sr_free(&e->a, t);
		return NULL;
	}
	se_objindex_register(&e->oi, &t->o);
	return &t->o;
}
