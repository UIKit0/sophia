
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
se_dbctl(seobj *o, va_list args)
{
	sedb *db = (sedb*)o;
	char *name = va_arg(args, char*);
	if (srlikely(strcmp(name, "ctl") == 0))
		return &db->objctl;
	if (srunlikely(strcmp(name, "conf") == 0))
		return &db->objconf.o;
	return NULL;
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
		// XXX
		return 0;
	}
	return se_dbfree(db);
}

static int
se_dbset(seobj *o, va_list args)
{
	sedb *db = (sedb*)o;
	if (srunlikely(! db->scheme.ready))
		return -1;
	return se_txssset(db, SVSET, args);
}

static int
se_dbdelete(seobj *o, va_list args)
{
	sedb *db = (sedb*)o;
	if (srunlikely(! db->scheme.ready))
		return -1;
	return se_txssset(db, SVDELETE, args);
}

static int
se_dbget(seobj *o, va_list args)
{
	sedb *db = (sedb*)o;
	if (srunlikely(! db->scheme.ready))
		return -1;
	return se_txssget(db, args);
}

static void*
se_dbbegin(seobj *o)
{
	sedb *db = (sedb*)o;
	if (srunlikely(! db->scheme.ready))
		return NULL;
	return se_txnew(db, NULL, NULL);
}

static void*
se_dbcursor(seobj *o, int order, void *key, int keysize)
{
	sedb *db = (sedb*)o;
	if (srunlikely(! db->scheme.ready))
		return NULL;
	return se_cursornew(db, order, key, keysize);
}

static int se_dbopen(seobj*, va_list);

static seobjif sedbif =
{
	.ctl       = se_dbctl,
	.database  = NULL,
	.open      = se_dbopen,
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
static seobjif sedbconfif;

static sedb*
se_dbmatch_parent(se *e, sedb *own, uint32_t dsn)
{
	se_objindex_lock(&e->oi);
	srlist *i;
	sr_listforeach(&e->oi.dblist, i) {
		sedb *db = srcast(i, sedb, o.olink);
		if (db == own)
			continue;
		if (db->scheme.recover == 0)
			continue;
		if (db->scheme.dsn == dsn) {
			se_objindex_unlock(&e->oi);
			return db;
		}
	}
	se_objindex_unlock(&e->oi);
	return NULL;
}

static int
se_dbopen(seobj *o, va_list args)
{
	(void)args;
	sedb *db = (sedb*)o;
	se *e = db->e;
	if (db->scheme.ready)
		return -1;
	sedb *prev = se_dbmatch_parent(e, db, db->scheme.dsn);
	int rc;
	if (prev == NULL) {
		/* new db */
		rc = sd_prepare(&db->db);
		if (srunlikely(rc == -1))
			return -1;
	} else {
		/* complete database recover using
		 * scheme */
		if (prev->scheme.ready)
			return -1;
		rc = sd_recover(&db->db, &prev->track);
		if (srunlikely(rc == -1)) {
			sd_free(&db->db);
			return -1;
		}
		ss_trackfreeindex(&prev->track);
		prev->scheme.dsn = UINT32_MAX;
		se_dbunref(prev);
	}
	rc = sm_init(&db->mvcc, &db->c);
	if (srunlikely(rc == -1)) {
		sd_free(&db->db);
		return -1;
	}
	db->scheme.ready = 1;
	return 0;
}

seobj *se_dbnew(se *e, uint32_t dsn)
{
	sedb *db = sr_malloc(&e->a, sizeof(sedb));
	if (srunlikely(db == NULL))
		return NULL;
	memset(db, 0, sizeof(sedb));
	se_objinit(&db->o, SEODB, &sedbif);
	sr_spinlockinit(&db->lock);
	db->ref    = 0;
	db->e      = e;
	int rc = sr_schemeinit(&db->scheme, &e->a);
	if (srunlikely(rc == -1)) {
		sr_free(&e->a, db);
		return NULL;
	}
	db->scheme.dsn = dsn;
	if (e->mode == SE_RECOVER)
		db->scheme.recover = 1;
	db->c      = e->c;
	db->c.sdb  = &db->scheme;
	rc = sd_new(&db->db, &db->scheme, &e->store, &db->c);
	if (srunlikely(rc == -1)) {
		sr_schemefree(&db->scheme, &e->a);
		sr_free(&e->a, db);
		return NULL;
	}
	ss_trackinit(&db->track, &db->c);
	rc = ss_trackprepare(&db->track, 512);
	if (srunlikely(rc == -1)) {
		se_dbdestroy(&db->o);
		return NULL;
	}
	se_objinit(&db->objctl.o, SEODBCTL, &sedbctlif);
	db->objctl.parent = db;
	se_objinit(&db->objconf.o, SEODBCONF, &sedbconfif);
	db->objconf.parent = db;
	se_dbref(db);
	se_objindex_register(&e->oi, &db->o);
	return &db->o;
}

seobj *se_dbmatch(se *e, uint32_t dsn)
{
	se_objindex_lock(&e->oi);
	srlist *i;
	sr_listforeach(&e->oi.dblist, i) {
		sedb *db = srcast(i, sedb, o.olink);
		if (db->scheme.dsn == dsn) {
			se_objindex_unlock(&e->oi);
			return &db->o;
		}
	}
	se_objindex_unlock(&e->oi);
	return NULL;
}

int se_dbdrop(sedb *db)
{
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
	.database  = NULL,
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

static int
se_dbconfset(seobj *o, va_list args)
{
	sedbconf *c = (sedbconf*)o;
	if (srunlikely(c->parent->scheme.ready))
		return -1;
	srschemeparser sp;
	char *name = sr_schemeprepare(&sp, args);
	if (srunlikely(name == NULL))
		return -1;
	int rc = sr_schemeset(&sp, &c->parent->scheme);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

static seobjif sedbconfif =
{
	.ctl       = NULL,
	.database  = NULL,
	.open      = NULL,
	.destroy   = NULL,
	.set       = se_dbconfset,
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
