
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

static inline void *se_compaction(void *arg) 
{
	seprocess *self = arg;
	se *e = self->arg;
	while (se_active(e)) {
		setask *t = se_taskmgr_pop(&e->tm);
		if (t == NULL) {
			se_processwait(self);
			continue;
		}
		t->p = self;
		int rc = se_taskrun(t, e);
		if (srunlikely(rc == -1))
			break;
		assert(rc == 0);
		se_taskmgr_done(&e->tm, t);
		se_scheduledone(e, t);
	}
	return NULL;
}

static inline void *se_scheduler(void *arg) 
{
	seprocess *self = arg;
	se *e = self->arg;
	while (se_active(e)) {
		seplan plan;
		se_planinit(&plan, e, SE_ALL);
		int rc = se_schedule(&plan);
		if (srunlikely(rc == -1))
			break;
		se_processwait_timeout(self, e->conf.scheduler_tick);
	}
	return NULL;
}

static int
se_open(seobj *o, va_list args)
{
	(void)args;
	se *e = (se*)o;
	int rc = sr_confvalidate(&e->conf);
	if (srunlikely(rc == -1))
		return -1;
	sr_allocinit(&e->a, e->conf.a, e->conf.aarg);
	sr_cinit(&e->c, &e->conf, NULL, &e->seq, &e->a);
	se_taskmgr_init(&e->tm);
	se_processstub_init(&e->stub, &e->c, e);
	if (e->conf.logdir) {
		sl_poolinit(&e->lp, &e->c);
		rc = sl_poolopen(&e->lp);
		if (srunlikely(rc == -1))
			return -1;
	}
	rc = ss_open(&e->store, &e->c);
	if (srunlikely(rc == -1))
		return -1;
	int create = rc;
	if (create == 0) {
		e->mode = SE_RECOVER;
		rc = se_recover(e);
		if (srunlikely(rc == -1))
			return -1;
	} else {
		sr_seq(&e->seq, SR_LSNNEXT);
	}
	if (e->conf.logdir) {
		rc = sl_poolrotate(&e->lp);
		if (srunlikely(rc == -1))
			return -1;
	}
	e->mode = SE_ONLINE;
	se_processmgr_init(&e->pm);
	rc = se_processmgr_new(&e->pm, &e->c, e->conf.threads, se_compaction, e);
	if (srunlikely(rc == -1))
		return -1;
	if (e->conf.scheduler == 1) { /* parallel */
		rc = se_processmgr_new(&e->pm, &e->c, 1, se_scheduler, e);
		if (srunlikely(rc == -1))
			return -1;
	}
	return 0;
}

static inline int
se_destroylist(srlist *l)
{
	int rcret = 0;
	int rc;
	srlist *i, *n;
	sr_listforeach_safe(l, i, n) {
		seobj *o = srcast(i, seobj, olink);
		rc = sp_destroy(o);
		if (srunlikely(rc == -1))
			rcret = -1;
	}
	return rcret;
}

static int
se_destroy(seobj *o)
{
	se *e = (se*)o;
	int rcret = 0;
	int rc;
	e->mode = SE_SHUTDOWN;
	rc = se_processmgr_shutdown(&e->pm, &e->c);
	if (srunlikely(rc == -1))
		rcret = -1;

	se_destroylist(&e->oi.backuplist);
	se_destroylist(&e->oi.cursorlist);
	se_destroylist(&e->oi.txlist);
	se_destroylist(&e->oi.dblist);
	se_objindex_free(&e->oi);

	se_processstub_free(&e->stub, &e->c);
	rc = ss_close(&e->store, &e->c);
	if (srunlikely(rc == -1))
		rcret = -1;
	if (e->conf.logdir) {
		rc = sl_poolshutdown(&e->lp);
		if (srunlikely(rc == -1))
			rcret = -1;
	}
	se_taskmgr_free(&e->tm, &e->c);
	sr_flagsfree(&e->schedflags);
	sr_seqfree(&e->seq);
	sr_efree(&e->e);
	sra std;
	sr_allocinit(&std, sr_allocstd, NULL);
	sr_conffree(&e->conf, &std);
	free(e);
	return rcret;
}

static void*
se_database(seobj *o, va_list args)
{
	(void)args;
	se *e = (se*)o;
	uint32_t dsn = sr_seq(&e->seq, SR_DSNNEXT);
	return se_dbnew(e, dsn);
}

static void*
se_ctl(seobj *o, va_list args)
{
	se *e = (se*)o;
	char *name = va_arg(args, char*);
	if (srlikely(strcmp(name, "ctl") == 0))
		return &e->objctl;
	if (srunlikely(strcmp(name, "conf") == 0))
		return &e->objconf.o;
	return NULL;
}

static void*
se_backup(seobj *o)
{
	se *e = (se*)o;
	return se_backupnew(e);
}

static seobjif seif =
{
	.ctl       = se_ctl,
	.database  = se_database,
	.open      = se_open,
	.destroy   = se_destroy,
	.set       = NULL,
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
	.backup    = se_backup
};

static seobjif sectlif;
static seobjif seconfif;

seobj *se_new(void)
{
	se *e = malloc(sizeof(*e));
	if (srunlikely(e == NULL))
		return NULL;
	memset(e, 0, sizeof(*e));
	e->mode = SE_OFFLINE;
	se_objinit(&e->o, SEO, &seif);
	sr_einit(&e->e);
	sr_seqinit(&e->seq);
	se_objindex_init(&e->oi);
	sr_allocinit(&e->a, sr_allocstd, NULL);
	sr_cinit(&e->c, &e->conf, NULL, &e->seq, &e->a);

	ss_init(&e->store);
	int rc = sr_confinit(&e->conf, &e->a);
	if (srunlikely(rc == -1)) {
		sr_free(&e->a, e);
		return NULL;
	}
	sr_flagsinit(&e->schedflags, 0);
	se_objinit(&e->objconf.o, SEOCONF, &seconfif);
	e->objconf.e = e;
	se_objinit(&e->objctl.o, SEOCTL, &sectlif);
	e->objctl.e = e;
	return &e->o;
}

static int
se_ctlset(seobj *o, va_list args)
{
	sectl *c = (sectl*)o;
	char *name = va_arg(args, char*);
	seplan p;
	memset(&p, 0, sizeof(p));
	if (strcmp(name, "schedule") == 0) {
		p.plan = SE_ALL;
	} else
	if (strcmp(name, "snapshot") == 0) {
		p.plan = SE_SNAPSHOT | SE_FORCE;
	} else {
		return -1;
	}
	p.e = c->e;
	return se_schedule(&p);
}

static int
se_confset(seobj *o, va_list args)
{
	seconf *c = (seconf*)o;
	char *path = va_arg(args, char*);
	if (srunlikely(se_active(c->e)))
		return -1;
	int rc = sr_confset(&c->e->conf, &c->e->a, path, args);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

static seobjif sectlif =
{
	.ctl       = NULL,
	.database  = NULL,
	.open      = NULL,
	.destroy   = NULL,
	.set       = se_ctlset,
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

static seobjif seconfif =
{
	.ctl       = NULL,
	.database  = NULL,
	.open      = NULL,
	.destroy   = NULL,
	.set       = se_confset,
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

int se_list(se *e, srbuf *list)
{
	se_objindex_lock(&e->oi);
	srlist *i;
	sr_listforeach(&e->oi.dblist, i) {
		sedb *db = srcast(i, sedb, o.olink);
		se_dbref(db);
		int rc = sr_bufadd(list, &e->a, &db, sizeof(void**));
		if (srunlikely(rc == -1)) {
			se_dbunref(db);
			se_objindex_unlock(&e->oi);
			return -1;
		}
	}
	se_objindex_unlock(&e->oi);
	return 0;
}

int se_listunref(se *e, srbuf *list)
{
	int rcret = 0;
	sriter i;
	sr_iterinit(&i, &sr_bufiterref, &e->c);
	sr_iteropen(&i, list, sizeof(sedb**));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sedb *db = sr_iterof(&i);
		int rc = se_dbunref(db);
		if (srunlikely(rc == -1))
			rcret = -1;
	}
	sr_buffree(list, &e->a);
	return rcret;
}

int se_snapshot(se *e, ssc *sc)
{
	ss_creset(sc);
	int rc = ss_snapshot(&sc->build);
	if (srunlikely(rc == -1))
		return -1;

	rc = ss_listref(&e->store, &e->a, &sc->build.vbuf);
	if (srunlikely(rc == -1))
		return -1;

	ss_lock(&e->store);

	srbuf list;
	sedb reserve[16];
	sr_bufinit_reserve(&list, reserve, sizeof(reserve));
	rc = se_list(e, &list);
	if (srunlikely(rc == -1)) {
		ss_unlock(&e->store);
		return -1;
	}

	sriter i;
	sr_iterinit(&i, &sr_bufiterref, &e->c);
	sr_iteropen(&i, &list, sizeof(sedb**));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sedb *db = sr_iterof(&i);
		rc = sd_snapshot(&db->db, sc);
		if (srunlikely(rc == -1)) {
			se_listunref(e, &list);
			ss_unlock(&e->store);
			return -1;
		}
	}

	rc = ss_snapshotcommit(&sc->build);
	if (srunlikely(rc == -1)) {
		se_listunref(e, &list);
		ss_unlock(&e->store);
		return -1;
	}

	sswrite w;
	ss_writeinit(&w, 1, NULL, &sc->build, NULL, 0);
	w.lock = 0;
	rc = ss_write(&e->store, &w);

	se_listunref(e, &list);
	ss_unlock(&e->store);
	return rc;
}
