
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

static int se_dbfree(sedb*);

void se_dbref(sedb *db)
{
	sr_spinlock(&db->lock);
	db->ref++;
	sr_spinunlock(&db->lock);
}

int se_dbunref(sedb *db)
{
	sr_spinlock(&db->lock);
	int ref = --db->ref;
	sr_spinunlock(&db->lock);
	if (srunlikely(ref > 0))
		return 0;
	return se_dbfree(db);
}

static void*
se_dbctl(seobj *o)
{
	sedb *db = (sedb*)o;
	return &db->objctl;
}

static int
se_dbfree(sedb *db)
{
	se_objindex_unregister(&db->e->oi, &db->o);
	int rcret = 0;
	int rc = sm_free(&db->mvcc);
	if (srunlikely(rc == -1))
		rcret = -1;
	rc = sd_free(&db->db);
	if (srunlikely(rc == -1))
		rcret = -1;
	ss_trackfree(&db->track);
	sr_schemefree(&db->scheme, &db->e->a);
	sr_spinlockfree(&db->lock);
	sr_free(&db->e->a, db);
	return rcret;
}

static int
se_dbdestroy(seobj *o)
{
	sedb *db = (sedb*)o;
	if (se_active(db->e)) {
		sm_dbindex_unuse(&db->e->dbvi, &db->dbv);
		return 0;
	}
	return se_dbfree(db);
}

static int
se_dbset(seobj *o, va_list args)
{
	sedb *db = (sedb*)o;
	return se_txssset(db, SVSET, args);
}

static int
se_dbdelete(seobj *o, va_list args)
{
	sedb *db = (sedb*)o;
	return se_txssset(db, SVDELETE, args);
}

static int
se_dbget(seobj *o, va_list args)
{
	sedb *db = (sedb*)o;
	return se_txssget(db, args);
}

static void*
se_dbbegin(seobj *o)
{
	sedb *db = (sedb*)o;
	return se_txnew(db, NULL, NULL);
}

static void*
se_dbcursor(seobj *o, int order, void *key, int keysize)
{
	sedb *db = (sedb*)o;
	return se_cursornew(db, order, key, keysize);
}

static seobjif sedbif =
{
	.ctl       = se_dbctl,
	.use       = NULL,
	.open      = NULL,
	.destroy   = se_dbdestroy,
	.set       = se_dbset,
	.del       = se_dbdelete,
	.get       = se_dbget,
	.begin     = se_dbbegin,
	.commit    = NULL,
	.rollback  = NULL,
	.cursor    = se_dbcursor,
	.fetch     = NULL,
	.key       = NULL,
	.keysize   = NULL,
	.value     = NULL,
	.valuesize = NULL,
	.backup    = NULL
};

static seobjif sedbctlif;

seobj *se_dbnew(se *e, srscheme *s)
{
	sedb *db = sr_malloc(&e->a, sizeof(sedb));
	if (srunlikely(db == NULL))
		return NULL;
	se_objinit(&db->o, SEODB, &sedbif);
	sr_spinlockinit(&db->lock);
	db->ref    = 0;
	db->e      = e;
	db->scheme = *s;
	db->c      = e->c;
	db->c.sdb  = &db->scheme;
	ss_trackinit(&db->track, &db->c);
	int rc = sm_init(&db->mvcc, &db->c);
	if (srunlikely(rc == -1)) {
		sr_schemefree(&db->scheme, &e->a);
		sr_free(&e->a, db);
		return NULL;
	}
	rc = sd_new(&db->db, &db->scheme, &e->store, &db->c);
	if (srunlikely(rc == -1)) {
		sm_free(&db->mvcc);
		sr_schemefree(&db->scheme, &e->a);
		sr_free(&e->a, db);
		return NULL;
	}
	rc = sd_prepare(&db->db);
	if (srunlikely(rc == -1)) {
		sd_free(&db->db);
		sm_free(&db->mvcc);
		sr_schemefree(&db->scheme, &e->a);
		sr_free(&e->a, db);
		return NULL;
	}
	sm_dbinit(&db->dbv, &db->scheme, db);
	sm_dbindex_add(&e->dbvi, &db->dbv);

	se_objinit(&db->objctl.o, SEODBCTL, &sedbctlif);
	db->objctl.parent = db;

	se_dbref(db);
	se_objindex_register(&e->oi, &db->o);
	return &db->o;
}

seobj *se_dbprepare(se *e, uint32_t dsn)
{
	srscheme s;
	int rc = sr_schemeinit_undef(&s, &e->a, &e->ci);
	if (srunlikely(rc == -1))
		return NULL;
	s.dsn = dsn;
	sedb *db = sr_malloc(&e->a, sizeof(sedb));
	if (srunlikely(db == NULL))
		return NULL;
	se_objinit(&db->o, SEODB, &sedbif);
	sr_spinlockinit(&db->lock);
	db->ref    = 0;
	db->e      = e;
	db->scheme = s;
	db->c      = e->c;
	db->c.sdb  = &db->scheme;
	ss_trackinit(&db->track, &db->c);
	rc = sm_init(&db->mvcc, &db->c);
	if (srunlikely(rc == -1)) {
		sr_schemefree(&db->scheme, &e->a);
		sr_free(&e->a, db);
		return NULL;
	}
	rc = sd_new(&db->db, &db->scheme, &e->store, &db->c);
	if (srunlikely(rc == -1)) {
		sm_free(&db->mvcc);
		sr_schemefree(&db->scheme, &e->a);
		sr_free(&e->a, db);
		return NULL;
	}
	rc = ss_trackprepare(&db->track, 512);
	if (srunlikely(rc == -1)) {
		se_dbdestroy(&db->o);
		return NULL;
	}

	se_objinit(&db->objctl.o, SEODBCTL, &sedbctlif);
	db->objctl.parent = db;

	se_dbref(db);
	se_objindex_register(&e->oi, &db->o);
	return &db->o;
}

seobj *se_dbdeploy(sedb *db, srscheme *s)
{
	sr_schemefree(&db->scheme, &db->e->a);
	db->scheme = *s;
	sm_dbinit(&db->dbv, &db->scheme, db);
	sm_dbindex_add(&db->e->dbvi, &db->dbv);
	return &db->o;
}

seobj *se_dbmatch(se *e, char *name)
{
	se_objindex_lock(&e->oi);
	srlist *i;
	sr_listforeach(&e->oi.dblist, i) {
		sedb *db = srcast(i, sedb, o.olink);
		if (strcmp(db->db.scheme->name, name) == 0) {
			se_objindex_unlock(&e->oi);
			return &db->o;
		}
	}
	se_objindex_unlock(&e->oi);
	return NULL;
}

seobj *se_dbmatchid(se *e, uint32_t dsn)
{
	se_objindex_lock(&e->oi);
	srlist *i;
	sr_listforeach(&e->oi.dblist, i) {
		sedb *db = srcast(i, sedb, o.olink);
		if (db->db.id == dsn) {
			se_objindex_unlock(&e->oi);
			return &db->o;
		}
	}
	se_objindex_unlock(&e->oi);
	return NULL;
}

int se_dbdrop(sedb *db)
{
	sm_dbindex_remove(&db->e->dbvi, &db->dbv);
	se_dbunref(db);
	return 0;
}

static int
se_dbctlset(seobj *o, va_list args)
{
	sedbctl *c = (sedbctl*)o;
	se *e = c->parent->e;
	char *name = va_arg(args, char*);
	seplan p;
	memset(&p, 0, sizeof(p));
	if (strcmp(name, "merge") == 0) {
		p.plan = SE_MERGEDB;
		p.db = c->parent;
	} else
	if (strcmp(name, "logrotate") == 0) {
		p.plan = SE_ROTATELOG;
	} else
	if (strcmp(name, "dbrotate") == 0) {
		p.plan = SE_ROTATEDB;
	} else {
		return -1;
	}
	p.plan |= SE_FORCE;
	p.e = e;
	return se_schedule(&p);
}

static seobjif sedbctlif =
{
	.ctl       = NULL,
	.use       = NULL,
	.open      = NULL,
	.destroy   = NULL,
	.set       = se_dbctlset,
	.get       = NULL,
	.del       = NULL,
	.begin     = NULL,
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
