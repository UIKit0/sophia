
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libss.h>

typedef struct {
	ssref *r;
	ssdb *db;
} sscopyref;

int ss_copyinit(sscopy *copy, ss *s, src *c, ssc *sc, ssdblist *dbl, ssindex *i)
{
	copy->c      = c;
	copy->s      = s;
	copy->dbl    = dbl;
	copy->index  = i;
	ss_creset(sc);
	copy->rwbuf  = &sc->a;
	copy->ref    = &sc->b;
	return 0;
}

int ss_copy(sscopy *c)
{
	int count = 0;
	sriter i;
	sr_iterinit(&i, &ss_indexiterraw, c->c);
	sr_iteropen(&i, c->index);
	while (sr_iterhas(&i))
	{
		ssref *p = sr_iterof(&i);
		ssdb *db = ss_dblist_in(c->dbl, p->dfsn);
		if (db) {
			int rc = sr_bufensure(c->rwbuf, c->c->a, p->size);
			if (srunlikely(rc == -1))
				return -1;
			rc = sr_filepread(&db->file, p->offset, c->rwbuf->p, p->size);
			if (srunlikely(rc == -1))
				return -1;
			sr_bufadvance(c->rwbuf, p->size);
			rc = sr_bufensure(c->ref, c->c->a, sizeof(sscopyref));
			if (srunlikely(rc == -1))
				return -1;
			sscopyref *cr = (sscopyref*)c->ref->p;
			cr->db = db;
			cr->r  = p;
			sr_bufadvance(c->ref, sizeof(sscopyref));
			count++;
		}
		sr_iternext(&i);
	}
	return count;
}

int ss_copywrite(sscopy *c, sswritef cb, void *cbarg)
{
	int count = sr_bufused(c->ref) / sizeof(sscopyref);
	assert(count > 0);
	sswrite w;
	ss_writeinit(&w, 1, NULL, NULL, c->rwbuf, count);
	ss_writeinit_callback(&w, cb, cbarg);
	int rc = ss_write(c->s, c->c, &w);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

int ss_copycommit(sscopy *c, uint32_t dfsn, uint64_t offset)
{
	/*sr_spinlock(&c->s->lock);*/
	sriter i;
	sr_iterinit(&i, &sr_bufiter, NULL);
	sr_iteropen(&i, c->ref, sizeof(sscopyref));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sscopyref *r = sr_iterof(&i);
		r->r->dfsn   = dfsn;
		r->r->offset = offset;
		offset += r->r->size;
		ss_indexaccount(c->index, r->r);
		sr_gcsweep(&r->db->gc, 1);
	}
	/*sr_spinunlock(&c->s->lock);*/
	return 0;
}
