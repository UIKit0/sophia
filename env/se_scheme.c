
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

static void*
se_schemectl(seobj *o)
{
	sescheme *s = (sescheme*)o;
	return sp_ctl(s->scheme);
}

static int
se_schemetxrollback(seobj *o)
{
	seschemetx *t = (seschemetx*)o;
	int rc = sp_rollback(t->t);
	se_objindex_unregister(&t->s->e->oi, &t->o);
	sr_free(&t->s->e->a, t);
	return rc;
}

static int
se_schemetxcommit(seobj *o, va_list args)
{
	seschemetx *t = (seschemetx*)o;
	sedb *db = t->t->db;
	int rc = t->t->o.oif->commit(&t->t->o, args);
	switch (rc) {
	case 2: /* wait */
		return rc;
	case 0: /* commit */
		/* force scheme db merge */
		rc = sp_set(sp_ctl(db), "merge");
		break;
	default:
		break;
	}
	se_objindex_unregister(&t->s->e->oi, &t->o);
	sr_free(&t->s->e->a, t);
	return rc;
}

static int
se_schemetx_postcommit(setx *t, void *arg srunused)
{
	se *e = t->db->e;
	sriter i;
	sr_iterinit(&i, &sr_bufiter, &t->db->c);
	sr_iteropen(&i, &t->t.log.buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *v = sr_iterof(&i);
		uint8_t flags = svflags(v);
		int rc;
		srscheme s;
		rc = sr_schemeinit_from(&s, &e->a, &e->ci,
		                        svvalue(v),
		                        svvaluesize(v));
		if (srunlikely(rc == -1))
			return -1;

		if (flags & SVSET)
		{
			s.lsnc = svlsn(v);
			seobj *db = se_dbnew(e, &s);
			if (srunlikely(db == NULL)) {
				sr_schemefree(&s, &e->a);
				return -1;
			}
		} else
		if (flags & SVDELETE)
		{
			/* match db by id and set drop lsn.
			 *
			 * do nothing if db has been created and
			 * deleted during tx.
			 */
			sm_dbindex_drop(&e->dbvi, s.dsn, svlsn(v));
			sr_schemefree(&s, &e->a);
		}
	}
	return 0;
}

static inline int
se_schemetx_constraint(char *name)
{
	if (srunlikely(strcmp(name, "conf") == 0))
		return -1;
	else
	if (srunlikely(strcmp(name, "cmp") == 0))
		return -1;
	else
	if (srunlikely(strcmp(name, "scheme") == 0))
		return -1;
	return 0;
}

static int
se_schemetx_recover(seobj *o, va_list args, uint8_t flags)
{
	seschemetx *t = (seschemetx*)o;
	se *e = t->s->e;
	srscheme s;
	sv *v = va_arg(args, sv*);
	int rc = sr_schemeinit_from(&s, &e->a, &e->ci,
	                            svvalue(v),
	                            svvaluesize(v));
	if (srunlikely(rc == -1))
		return -1;
	srbuf buf;
	rc = sr_schemeserialize(&s, &e->a, &buf);
	if (srunlikely(rc == -1)) {
		sr_schemefree(&s, &e->a);
		return -1;
	}
	svlocal l;
	memset(&l, 0, sizeof(l));
	l.key       = svkey(v);
	l.keysize   = svkeysize(v);
	l.flags     = flags;
	l.value     = buf.s;
	l.valuesize = sr_bufused(&buf);
	sv lv;
	svinit(&lv, &sv_localif, &l, NULL);

	sm_lock(&t->t->db->mvcc);
	rc = sm_set(&t->t->t, &lv);
	sm_unlock(&t->t->db->mvcc);

	sr_buffree(&buf, &e->a);
	sr_schemefree(&s, &e->a);
	return rc;
}

static int
se_schemetxset(seobj *o, va_list args)
{
	seschemetx *t = (seschemetx*)o;
	se *e = t->s->e;
	if (e->mode == SE_RECOVER)
		return se_schemetx_recover(o, args, SVSET);

	srschemeparser sp;
	char *name = sr_schemeprepare(&sp, args);
	if (srunlikely(name == NULL))
		return -1;
	svlocal l;
	memset(&l, 0, sizeof(l));
	l.key = name;
	l.keysize = strlen(name);
	sv v;
	svinit(&v, &sv_localif, &l, NULL);

	sm_lock(&t->t->db->mvcc);
	/* check name contraints */
	if (srunlikely(se_schemetx_constraint(name) == -1))
		goto e0;
	smv *current;
	int rc = sm_match(&t->t->t, &v, &current);
	if (srunlikely(rc == -1))
		goto e0;

	srscheme s;
	if (rc == 0) {
		/* read from smdbindex */
		int has = sm_dbindex_land(&e->dbvi, name, t->t->t.lsvn, NULL);
		if (! has) {
			/* create scheme with default configuration */
			rc = sr_schemeinit(&s, &e->a, name, &e->ci, "string"); 
			s.dsn = sr_seq(&e->seq, SR_DSNNEXT);
		} else {
			/* deny alter */
			goto e0;
		}
	} else {
		/* alter scheme (not commited) */
		rc = sr_schemeinit_from(&s, &e->a, &e->ci, sm_vv(current),
		                        current->valuesize);
	}
	if (srunlikely(rc == -1))
		goto e0;
	rc = sr_schemeset(&sp, &e->ci, &s);
	if (srunlikely(rc == -1))
		goto e1;

	srbuf buf;
	rc = sr_schemeserialize(&s, &e->a, &buf);
	if (srunlikely(rc == -1))
		goto e1;
	l.flags = SVSET;
	l.value = buf.s;
	l.valuesize = sr_bufused(&buf);
	rc = sm_set(&t->t->t, &v);
	sm_unlock(&t->t->db->mvcc);
	sr_buffree(&buf, &e->a);
	sr_schemefree(&s, &e->a);
	return rc;
e1:
	sr_schemefree(&s, &e->a);
e0:
	sm_unlock(&t->t->db->mvcc);
	return -1;
}

static int
se_schemetxdel(seobj *o, va_list args)
{
	seschemetx *t = (seschemetx*)o;
	se *e = t->s->e;
	if (e->mode == SE_RECOVER)
		return se_schemetx_recover(o, args, SVDELETE);

	srschemeparser sp;
	char *name = sr_schemeprepare(&sp, args);
	if (srunlikely(name == NULL))
		return -1;
	svlocal l;
	memset(&l, 0, sizeof(l));
	l.key = name;
	l.keysize = strlen(name);
	sv v;
	svinit(&v, &sv_localif, &l, NULL);

	sm_lock(&t->t->db->mvcc);
	/* check name contraints */
	if (srunlikely(se_schemetx_constraint(name) == -1))
		goto e0;
	smv *current;
	int rc = sm_match(&t->t->t, &v, &current);
	if (srunlikely(rc == -1))
		goto e0;

	srscheme s;
	if (rc == 0) {
		/* read from smdbindex */
		uint32_t dsn;
		int has = sm_dbindex_land(&e->dbvi, name, t->t->t.lsvn, &dsn);
		if (srunlikely(! has))
			goto e0;
		/* create scheme with default configuration */
		rc = sr_schemeinit(&s, &e->a, name, &e->ci, "string"); 
		if (srunlikely(rc == -1))
			goto e0;
		s.dsn = dsn;
	} else {
		/* alter scheme (not commited) */
		rc = sr_schemeinit_from(&s, &e->a, &e->ci, sm_vv(current),
		                        current->valuesize);
	}
	srbuf buf;
	rc = sr_schemeserialize(&s, &e->a, &buf);
	if (srunlikely(rc == -1))
		goto e1;
	l.flags = SVDELETE;
	l.value = buf.s;
	l.valuesize = sr_bufused(&buf);
	rc = sm_set(&t->t->t, &v);
	sm_unlock(&t->t->db->mvcc);
	sr_buffree(&buf, &e->a);
	sr_schemefree(&s, &e->a);
	return rc;
e1:
	sr_schemefree(&s, &e->a);
e0:
	sm_unlock(&t->t->db->mvcc);
	return -1;
}

static seobjif seschemetxif =
{
	.ctl       = NULL,
	.use       = NULL,
	.open      = NULL,
	.destroy   = se_schemetxrollback,
	.set       = se_schemetxset,
	.get       = NULL,
	.del       = se_schemetxdel,
	.begin     = NULL,
	.commit    = se_schemetxcommit,
	.rollback  = se_schemetxrollback,
	.cursor    = NULL,
	.fetch     = NULL,
	.key       = NULL,
	.keysize   = NULL,
	.value     = NULL,
	.valuesize = NULL,
	.backup    = NULL
};

static inline seobj*
se_schemetxnew(sescheme *s)
{
	seschemetx *t = sr_malloc(&s->e->a, sizeof(seschemetx));
	if (srunlikely(t == NULL))
		return NULL;
	se_objinit(&t->o, SEOSCHEMETX, &seschemetxif);
	t->s = s;
	t->t = (setx*)se_txnew(s->scheme, se_schemetx_postcommit, t);
	if (srunlikely(t->t == NULL)) {
		sr_free(&s->e->a, t);
		return NULL;
	}
	se_objindex_register(&t->s->e->oi, &t->o);
	return &t->o;
}

static void*
se_schemebegin(seobj *o)
{
	sescheme *s = (sescheme*)o;
	return se_schemetxnew(s);
}

static int
se_schemedestroy(seobj *o)
{
	sescheme *s = (sescheme*)o;
	if (se_active(s->e))
		return 0;
	s->scheme = NULL;
	sr_free(&s->e->a, s);
	return 0;
}

static seobjif seschemeif =
{
	.ctl       = se_schemectl,
	.use       = NULL,
	.open      = NULL,
	.destroy   = se_schemedestroy,
	.set       = NULL,
	.get       = NULL,
	.del       = NULL,
	.begin     = se_schemebegin,
	.commit    = NULL,
	.rollback  = NULL,
	.cursor    = NULL,
	.fetch     = NULL,
	.key       = NULL,
	.keysize   = NULL,
	.value     = NULL,
	.valuesize = NULL,
	.backup    = NULL
};

static inline int
se_schemesetup(sescheme *s)
{
	sedb *db = s->scheme;
	srbuf buf;
	int rc = sr_schemeserialize(&db->scheme, &s->e->a, &buf);
	if (srunlikely(rc == -1))
		return -1;
	void *tx = sp_begin(db);
	if (srunlikely(tx == NULL)) {
		sr_buffree(&buf, &s->e->a);
		return -1;
	}
	rc = sp_set(tx, "scheme", 6, buf.s, sr_bufused(&buf));
	if (srunlikely(rc != 0)) {
		sr_buffree(&buf, &s->e->a);
		sp_rollback(tx);
		return -1;
	}
	sr_buffree(&buf, &s->e->a);
	rc = sp_commit(tx);
	if (srunlikely(rc != 0))
		return -1;

	/* force to write changes to db.
	 *
	 * this is only needed for recover from db with no or 
	 * without logs.
	*/
	return sp_set(sp_ctl(db), "merge");
}

seobj*
se_schemenew(se *e)
{
	sescheme *s = sr_malloc(&e->a, sizeof(sescheme));
	if (srunlikely(s == NULL))
		return NULL;
	se_objinit(&s->o, SEOSCHEME, &seschemeif);
	s->e = e;
	assert(sr_seq(&e->seq, SR_DSN) == 0);
	assert(sr_seq(&e->seq, SR_LSN) == 0);
	srscheme sdb;
	memset(&sdb, 0, sizeof(sdb));
	int rc;
	rc = sr_schemeinit(&sdb, &e->a, "scheme", &e->ci, "string");
	if (srunlikely(rc == -1)) {
		sr_free(&e->a, s);
		return NULL;
	}
	sdb.dsn = 0;
	sr_seq(&e->seq, SR_DSNNEXT);
	sedb *db = (sedb*)se_dbnew(e, &sdb);
	if (srunlikely(db == NULL)) {
		sr_free(&e->a, s);
		sr_schemefree(&sdb, &e->a);
		return NULL;
	}
	s->scheme = db;
	rc = se_schemesetup(s);
	if (srunlikely(rc == -1)) {
		sp_destroy(s->scheme);
		sr_free(&e->a, s);
		return NULL;
	}
	return &s->o;
}

seobj*
se_schemeprepare(se *e)
{
	sescheme *s = sr_malloc(&e->a, sizeof(sescheme));
	if (srunlikely(s == NULL))
		return NULL;
	se_objinit(&s->o, SEOSCHEME, &seschemeif);
	s->e = e;
	assert(sr_seq(&e->seq, SR_DSN) == 0);
	assert(sr_seq(&e->seq, SR_LSN) == 0);
	sedb *db = (sedb*)se_dbprepare(e, 0);
	if (srunlikely(db == NULL)) {
		sr_free(&e->a, s);
		return NULL;
	}
	s->scheme = db;
	return &s->o;
}
