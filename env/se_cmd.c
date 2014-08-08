
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

static inline int
se_gc(se *e)
{
	if (srunlikely(se_objindex_count(&e->oi, SEOBACKUP) > 0))
		return 0;
	int rc;
	if (srlikely(e->conf.logdir && e->conf.log_gc )) {
		rc = sl_poolgc(&e->lp);
		if (srunlikely(rc == -1))
			return -1;
	}
	if (srlikely(e->conf.db_gc)) {
		rc = ss_gc(&e->store, &e->c);
		if (srunlikely(rc == -1))
			return -1;
	}
	return 0;
}

static int
se_cmdmerge(setask *t, se *e)
{
	seprocess *p = t->p;
	sedb *db = t->db;
	uint64_t lsvn = sm_lsvn(&db->mvcc);
	int rc = sd_merge(&db->db, t->node, &p->sc, lsvn);
	if (srunlikely(rc == -1))
		return -1;
	return se_gc(e);
}

static int
se_cmdgc(setask *t, se *e)
{
	seprocess *p = t->p;
	sedb *db = t->db;
	int rc = sd_gc(&db->db, t->node, &p->sc, t->gcl);
	if (srunlikely(rc == -1))
		return -1;
	return se_gc(e);
}

static int
se_cmdrotatelog(setask *t, se *e)
{
	(void)t;
	int rc = sl_poolrotate(&e->lp);
	if (srunlikely(rc == -1))
		return -1;
	return se_gc(e);
}

static int
se_cmdrotatedb(setask *t, se *e)
{
	(void)t;
	int rc = ss_rotate(&e->store, &e->c);
	if (srunlikely(rc == -1))
		return -1;
	return se_gc(e);
}

static int
se_cmddrop(setask *t, se *e)
{
	seprocess *p = t->p;
	sedb *db = t->db;
	int rc = sd_drop(&db->db, &p->sc);
	if (srunlikely(rc == -1))
		return -1;
	se_dbunref(db);
	return se_gc(e);
}

static int
se_cmdsnapshot(setask *t, se *e)
{
	seprocess *p = t->p;
	int rc = se_snapshot(e, &p->sc);
	if (srunlikely(rc == -1))
		return -1;
	return se_gc(e);
}

static secmd se_cmdlist[] =
{
	{ SE_CMDNONE,      NULL,                     NULL       },
	{ SE_CMDMERGE,     (secmdf)se_cmdmerge,     "merge"     },
	{ SE_CMDGC,        (secmdf)se_cmdgc,        "gc"        },
	{ SE_CMDROTATELOG, (secmdf)se_cmdrotatelog, "rotatelog" },
	{ SE_CMDROTATEDB,  (secmdf)se_cmdrotatedb,  "rotatedb"  },
	{ SE_CMDDROP,      (secmdf)se_cmddrop,      "drop"      },
	{ SE_CMDSNAPSHOT,  (secmdf)se_cmdsnapshot,  "snapshot"  }
};

secmd *se_cmdof(secmdid id)
{
	return &se_cmdlist[id];
}
