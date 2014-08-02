
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
se_cursorseek(secursor *c, void *key, int keysize)
{
	sm_lock(&c->db->mvcc);
	sriter i;
	sr_iterinit(&i, &sm_iter, &c->db->c);
	sr_iteropen(&i, &c->db->mvcc, c->order, key, keysize, c->t.id);
	sv *a = sr_iterof(&i);

	sdquery q;
	sd_queryopen(&q, &c->db->db, c->order, c->t.lsvn, key, keysize);
	sd_querybuf(&q, &c->db->db.rbuf);
	int rc = sd_query(&q);
	if (srunlikely(rc == -1)) {
		sd_queryclose(&q);
		sm_unlock(&c->db->mvcc);
		return -1;
	}

	sv *v = NULL;
	if (a && rc > 0) {
		rc = svcompare(a, &q.result, &c->db->c.sdb->cmp);
		switch (c->order) {
		case SR_LT:
		case SR_LTE: 
			if (rc <= 0) {
				v = a;
			} else {
				v = &q.result;
			}
			break;
		case SR_GT:
		case SR_GTE:
			if (rc >= 0) {
				v = a;
			} else {
				v = &q.result;
			}
			break;
		default: assert(0);
		}
	}
	if (a) {
		v = a;
	}
	if (rc) {
		v = &q.result;
	}
	if (v) {
		svinit(&c->v, &sv_localif, sv_copy(c->db->c.a, v), NULL);
	}
	sd_queryclose(&q);
	sm_unlock(&c->db->mvcc);
	return c->v.v != NULL;
}

static inline int
se_cursoropen(secursor *c, void *key, int keysize)
{
	int rc = sm_begin(&c->db->mvcc, &c->t);
	if (srunlikely(rc == -1))
		return -1;

	do {
		rc = se_cursorseek(c, key, keysize);
	} while (rc == 1 && (svflags(&c->v) & SVDELETE) > 0);

	if (srunlikely(rc == -1)) {
		sm_end(&c->t);
		return -1;
	}

	return c->v.v != NULL;
}

static int
se_cursordestroy(seobj *o)
{
	secursor *c = (secursor*)o;
	sm_end(&c->t);
	if (c->v.v) {
		svunref(&c->v, c->db->c.a);
		c->v.v = NULL;
	}
	se_objindex_unregister(&c->db->e->oi, &c->o);
	sr_free(&c->db->e->a, c);
	return 0;
}

static inline int
se_cursorfetch_do(seobj *o)
{
	secursor *c = (secursor*)o;
	if (srunlikely(c->ready)) {
		c->ready = 0;
		return c->v.v != NULL;
	}
	if (srunlikely(c->v.v == NULL))
		return 0;
	sv current = c->v;
	c->v.v = NULL;
	se_cursorseek(c, svkey(&current), svkeysize(&current));
	svunref(&current, c->db->c.a);
	return c->v.v != NULL;
}

static int
se_cursorfetch(seobj *o)
{
	secursor *c = (secursor*)o;
	int rc;
	do {
		rc = se_cursorfetch_do(o);
	} while (rc == 1 && (svflags(&c->v) & SVDELETE) > 0);
	return rc;
}

static void*
se_cursorkey(seobj *o)
{
	secursor *c = (secursor*)o;
	if (c->v.v == NULL)
		return NULL;
	return svkey(&c->v);
}

static size_t
se_cursorkeysize(seobj *o)
{
	secursor *c = (secursor*)o;
	if (c->v.v == NULL)
		return 0;
	return svkeysize(&c->v);
}

static void*
se_cursorvalue(seobj *o)
{
	secursor *c = (secursor*)o;
	if (c->v.v == NULL)
		return NULL;
	return svvalue(&c->v);
}

static size_t
se_cursorvaluesize(seobj *o)
{
	secursor *c = (secursor*)o;
	if (c->v.v == NULL)
		return 0;
	return svvaluesize(&c->v);
}

static seobjif secursorif =
{
	.ctl       = NULL,
	.database  = NULL,
	.open      = NULL,
	.destroy   = se_cursordestroy, 
	.set       = NULL,
	.get       = NULL,
	.del       = NULL,
	.begin     = NULL,
	.commit    = NULL,
	.rollback  = NULL,
	.cursor    = NULL,
	.fetch     = se_cursorfetch,
	.key       = se_cursorkey,
	.keysize   = se_cursorkeysize,
	.value     = se_cursorvalue,
	.valuesize = se_cursorvaluesize,
	.backup    = NULL
};

seobj *se_cursornew(sedb *db, int order, void *key, int keysize)
{
	se *e = db->e;
	secursor *c = sr_malloc(&e->a, sizeof(secursor));
	if (srunlikely(c == NULL))
		return NULL;
	se_objinit(&c->o, SEOCURSOR, &secursorif);
	c->db    = db;
	c->ready = 1;
	c->order = order;
	memset(&c->v, 0, sizeof(c->v));
	int rc = se_cursoropen(c, key, keysize);
	if (srunlikely(rc == -1)) {
		sr_free(&e->a, c);
		return NULL;
	}
	srorder o = SR_GTE;
	switch (c->order) {
	case SR_LT:
	case SR_LTE: o = SR_LT;
		break;
	case SR_GT:
	case SR_GTE: o = SR_GT;
		break;
	default: assert(0);
	}
	c->order = o;
	se_objindex_register(&e->oi, &c->o);
	return &c->o;
}
