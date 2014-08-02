
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
se_run(seplan *p, setask *t)
{
	if (p->plan & SE_FORCE || p->e->conf.threads == 0) {
		if (srunlikely(t->p == NULL))
			t->p = &p->e->stub;
		int rc = se_taskrun(t, p->e);
		se_scheduledone(p->e, t);
		return rc;
	}
	se_taskmgr_push(&p->e->tm, t);
	se_processmgr_wakeup(&p->e->pm);
	return 0;
}

static inline int
se_schedule_rotatelog(seplan *p)
{
	se *e = p->e;
	if (srunlikely(sr_flagsisset(&e->schedflags, SE_ROTATELOG)))
		return 0;
	int ready = (p->plan & SE_FORCE) > 0;
	if (! ready)
		ready = sl_poolrotate_ready(&e->lp, e->conf.log_rotate_wm);
	if (srunlikely(ready)) {
		if (srunlikely(sr_flagstryset(&e->schedflags, SE_ROTATELOG)))
			return 0;
		setask *t = se_tasknew(&e->a, se_cmdof(SE_CMDROTATELOG));
		if (srunlikely(t == NULL))
			return -1;
		return se_run(p, t);
	}
	return 0;
}

static inline int
se_schedule_rotatedb(seplan *p)
{
	se *e = p->e;
	if (srunlikely(sr_flagsisset(&e->schedflags, SE_ROTATEDB)))
		return 0;
	int ready = (p->plan & SE_FORCE) > 0;
	if (! ready)
		ready = ss_rotateready(&e->store, e->conf.db_rotate_wm);
	if (srunlikely(ready)) {
		if (srunlikely(sr_flagstryset(&e->schedflags, SE_ROTATEDB)))
			return 0;
		setask *t = se_tasknew(&e->a, se_cmdof(SE_CMDROTATEDB));
		if (srunlikely(t == NULL))
			return -1;
		return se_run(p, t);
	}
	return 0;
}

static inline int
se_schedule_node(seplan *p, sdstat *s, int drop, int round)
{
	se *e = p->e;
	if ((p->plan & SE_FORCE) || (drop && (s->i.n > 0)))
		return 1;
	if (s->i.n >= e->conf.node_merge_wm)
		return 1;
	if (round >= e->conf.node_merge_force_round &&
	    s->i.n >= e->conf.node_merge_force_min)
		return 1;
	return 0;
}

static inline int
se_schedule_nodemerge(seplan *p, sedb *db, sdnode *n, int drop, int *total)
{
	se *e = p->e;
	if (srunlikely(! se_active(e)))
		return 0;
	if (srlikely(sr_flagsisset(&n->flags, SD_FINPLAN)))
		return 0;
	sdstat s;
	sd_nodelock(n);
	sd_nodestat(n, &s);
	*total += s.i.n;
	if (s.i.n > 0)
		n->round++;
	int round = n->round;
	sd_nodeunlock(n);

	int rc = se_schedule_node(p, &s, drop, round);
	if (srlikely(rc == 0))
		return 0;
	if (srunlikely(sr_flagstryset(&n->flags, SD_FINPLAN)))
		return 0;
	setask *t = se_tasknew(&e->a, se_cmdof(SE_CMDMERGE));
	if (srunlikely(t == NULL))
		return -1;
	t->db = db;
	t->node = n;
	return se_run(p, t);
}

static inline int
se_schedule_nodegc(seplan *p, sedb *db, ssdblist *gcl, sdnode *n)
{
	se *e = p->e;
	if (srunlikely(! se_active(e)))
		return 0;
	if (srlikely(sr_flagsisset(&n->flags, SD_FINPLAN)))
		return 0;
	sd_nodelock(n);
	int in = sd_nodein(n, gcl);
	sd_nodeunlock(n);
	if (srlikely(in == 0))
		return 0;
	if (srunlikely(sr_flagstryset(&n->flags, SD_FINPLAN)))
		return 0;
	setask *t = se_tasknew(&e->a, se_cmdof(SE_CMDGC));
	if (srunlikely(t == NULL))
		return -1;
	t->db   = db;
	t->node = n;
	t->gcl  = gcl;
	ss_dblist_ref(gcl);
	return se_run(p, t);
}

static inline int
se_schedule_dbdrop(seplan *p, sedb *db)
{
	if (srunlikely(sr_flagstryset(&db->db.flags, SD_FDROP)))
		return 0;
	setask *t = se_tasknew(&p->e->a, se_cmdof(SE_CMDDROP));
	if (srunlikely(t == NULL))
		return -1;
	t->db = db;
	return se_run(p, t);
}

static inline int
se_schedule_db(seplan *p, sedb *db, ssdblist *gcl, int snapshot)
{
	se *e = p->e;
	sd_indexlock(&db->db.primary);
	if (! sd_indexisinit(&db->db.primary)) {
		sd_indexunlock(&db->db.primary);
		return 0;
	}
	sdnodeindex index;
	int rc = sd_nodeindex_clone(&db->db.primary.i, &e->a, &index);
	sd_indexunlock(&db->db.primary);
	if (srunlikely(rc == -1))
		return -1;

	(void)snapshot;

	int unmerged = 0;
	int drop = 0;
	if ((p->plan & SE_DROP) && !snapshot) {
		/*drop = sm_dbindex_garbage(&e->dbvi, &db->dbv);*/
		// XXX refof db == 0
		drop = 0;
	}

	int i;
	if ((p->plan & SE_MERGE) ||
	    (p->plan & SE_MERGEDB)) {
		i = 0;
		while (i < index.n) {
			sdnode *n = index.i[i];
			rc = se_schedule_nodemerge(p, db, n, drop, &unmerged);
			if (srunlikely(rc == -1))
				goto error;
			i++;
		}
	}

	if ((p->plan & SE_DROP) && drop && unmerged == 0) {
		assert((p->plan & SE_MERGE) > 0);
		rc = se_schedule_dbdrop(p, db);
		if (srunlikely(rc == -1))
			goto error;
	}

	if (gcl) {
		i = 0;
		while (i < index.n) {
			sdnode *n = index.i[i];
			rc = se_schedule_nodegc(p, db, gcl, n);
			if (srunlikely(rc == -1))
				goto error;
			i++;
		}
	}

error:
	sd_nodeindex_freeindex(&index, &e->a);
	return 0; 
}

static inline int
se_schedule_gc(seplan *p, ssdblist **gclp)
{
	se *e = p->e;
	if (srunlikely(e->conf.db_gc == 0))
		return 0;
	if (srunlikely(! se_active(e)))
		return 0;
	return ss_gcschedule(&e->store, &e->a,
	                     e->conf.db_gc_factor, gclp);
}

static inline int
se_schedule_snapshot(seplan *p, int *scheduled)
{
	se *e = p->e;
	if (srunlikely(sr_flagsisset(&e->schedflags, SE_SNAPSHOT)))
		return 0;
	int ndrop = se_taskmgr_ndrop(&e->tm);
	int n = ss_written(&e->store);
	int snapshot = (n - e->schedsnap) >= e->conf.snapshot_wm;
	if (p->plan & SE_FORCE)
		snapshot = 1;
	*scheduled = snapshot;
	if (!snapshot || ndrop > 0)
		return 0;
	if (srunlikely(sr_flagstryset(&e->schedflags, SE_SNAPSHOT)))
		return 0;
	e->schedsnap = n;
	setask *t = se_tasknew(&e->a, se_cmdof(SE_CMDSNAPSHOT));
	if (srunlikely(t == NULL))
		return -1;
	return se_run(p, t);
}

int se_schedule(seplan *p)
{
	se *e = p->e;
	int rc = 0;
	if (p->plan & SE_ROTATELOG) {
		if (e->conf.logdir) {
			rc = se_schedule_rotatelog(p);
			if (srunlikely(rc == -1))
				return -1;
		}
	}
	if (p->plan & SE_ROTATEDB) {
		rc = se_schedule_rotatedb(p);
		if (srunlikely(rc == -1))
			return -1;
	}
	ssdblist *gcl = NULL;
	if (p->plan & SE_GC) {
		rc = se_schedule_gc(p, &gcl);
		if (srunlikely(rc == -1))
			return -1;
		if (gcl)
			ss_dblist_ref(gcl);
	}
	int snapshot = 0;
	if (p->plan & SE_SNAPSHOT) {
		rc = se_schedule_snapshot(p, &snapshot);
		if (srunlikely(rc == -1))
			return -1;
	}
	if ((p->plan & SE_MERGE) || gcl)
	{
		srbuf list;
		sedb reserve[16];
		sr_bufinit_reserve(&list, reserve, sizeof(reserve));
		rc = se_list(e, &list);
		if (srunlikely(rc == -1))
			return -1;
		sriter i;
		sr_iterinit(&i, &sr_bufiterref, &e->c);
		sr_iteropen(&i, &list, sizeof(sedb**));
		for (; sr_iterhas(&i); sr_iternext(&i)) {
			sedb *db = sr_iterof(&i);
			rc = se_schedule_db(p, db, gcl, snapshot);
			if (srunlikely(rc == -1)) {
				se_listunref(e, &list);
				goto error;
			}
		}
		se_listunref(e, &list);
	}
	if (p->plan & SE_MERGEDB) {
		rc = se_schedule_db(p, p->db, gcl, snapshot);
		if (srunlikely(rc == -1))
			goto error;
	}
error:
	if (srunlikely(gcl))
		ss_dblist_unref(gcl, &e->a);
	return rc;
}

int se_scheduledone(se *e, setask *t)
{
	switch (t->c->id) {
	case SE_CMDROTATELOG:
		sr_flagsunset(&e->schedflags, SE_ROTATELOG);
		break;
	case SE_CMDROTATEDB:
		sr_flagsunset(&e->schedflags, SE_ROTATEDB);
		break;
	case SE_CMDMERGE:
	case SE_CMDGC:
		sr_flagsunset(&t->node->flags, SD_FINPLAN);
		sd_nodelock(t->node);
		t->node->round = 0;
		sd_nodeunlock(t->node);
		break;
	case SE_CMDDROP:
		/* do nothing
		 * t->db no more exists.
		*/
		break;
	default: break;
	}
	se_taskfree(t, &e->a);
	return 0;
}
