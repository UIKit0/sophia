
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

int sd_begin(sd *db, sdtx *t, uint64_t lsvn, svlog *log, sl *l)
{
	sr_bufinit_reserve(&t->buf, t->reserve, sizeof(t->reserve));
	t->db = db;
	t->log = log;
	t->last = NULL;
	t->l = l;
	t->lsvn = lsvn;
	sd_indexlock(&db->primary);
	return 0;
}

int sd_end(sdtx *t)
{
	sr_buffree(&t->buf, t->db->c->a);
	return 0;
}

int sd_commit(sdtx *t)
{
	sd_indexunlock(&t->db->primary);
	return 0;
}

int sd_rollback(sdtx *t)
{
	(void)t;

	/* iterate vlog TILL last successful log
	 * position and do rollback */

	/* iterate and unlock all nodes from log */
	return 0;
}

int sd_write(sdtx *t)
{
	sriter i;
	sr_iterinit(&i, &sr_bufiter, t->db->c);
	sr_iteropen(&i, &t->log->buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *v = sr_iterof(&i);
		smv *mv = v->v;
		mv->log = t->l;

		/* prepare stat */
		sdstat *stat;
		int rc = sr_bufensure(&t->buf, t->db->c->a, sizeof(sdstat));
		if (srunlikely(rc == -1))
			return -1;
		stat = (sdstat*)t->buf.p;
		sr_bufadvance(&t->buf, sizeof(sdstat));

		/* match node and do replace */

		sriter r;
		sr_iterinit(&r, &sd_indexiter, t->db->c);
		sr_iteropen(&r, &t->db->primary, SR_LTE,
		            mv->key, mv->keysize);
		sdnode *n = sr_iterof(&r);
		assert(n != NULL);

		sd_nodelock(n);
		stat->node = n;
		rc = sd_pairreplace(&n->ip, t->db->c->a, stat, t->lsvn, mv);
		sd_nodeunlock(n);
		if (srunlikely(rc == -1))
			return -1;
		t->last = v;
	}
	return 0;
}


#if 0
int sd_write_single(sd *db, uint64_t lsvn, sl *l, sv *v, sdstat *stat)
{
	smv *dv = sd_valloc(db->c->a, v);
	if (srunlikely(dv == NULL))
		return -1;
	dv->log = l;

	sd_nodeindex_lock(&db->primary.i);
	sdnode *n = sd_nodeindex_route(&db->primary.i, dv->key, dv->keysize);
	assert(n != NULL);
	sd_nodeindex_unlock(&db->primary.i);
	stat->node = n;

	int rc = sd_vindexpair_replace(&n->ip, db->c->a, stat, lsvn, dv);
	sr_spinunlock(&n->lock);

	if (srunlikely(rc == -1)) {
		sr_free(db->c->a, dv);
		return -1;
	}
	return rc;
}
#endif
