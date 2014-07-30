
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

static inline void
ss_attach(ss *s, ssdb *db)
{
	sr_listappend(&s->list, &db->link);
	s->n++;
	ss_dbref(db);
}

int ss_init(ss *s)
{
	sr_spinlockinit(&s->lock);
	sr_mutexinit(&s->lockw);
	sr_listinit(&s->list);
	s->n = 0;
	s->nwrite = 0;
	return 0;
}

static inline int
ss_create(ss *s, src *c)
{
	int rc;
	if (! c->c->dir_create)
		return -1;
	if (! c->c->dir_write)
		return -1;
	rc = sr_filemkdir(c->c->dir);
	if (srunlikely(rc == -1))
		return -1;
	ssdb *db = ss_dbnew(c);
	if (srunlikely(db == NULL))
		return -1;
	ss_attach(s, db);
	return 1;
}

static inline int
ss_recover(ss *s, src *c)
{
	srbuf list;
	sr_bufinit(&list);
	srstoretype types[] = {
		{ "db", 1, 0 },
		{ NULL, 0, 0 }
	};
	int rc = sr_storeread(&list, c->a, types, c->c->dir);
	if (srunlikely(rc == -1))
		return -1;
	sriter i;
	sr_iterinit(&i, &sr_bufiter, c);
	sr_iteropen(&i, &list, sizeof(srstoreid));
	while(sr_iterhas(&i)) {
		srstoreid *id = sr_iterof(&i);
		ssdb *db = ss_dbopen(c, id->id);
		if (srunlikely(db == NULL)) {
			sr_buffree(&list, c->a);
			return -1;
		}
		ss_attach(s, db);
		sr_iternext(&i);
	}
	sr_buffree(&list, c->a);
	if (s->n) {
		ssdb *last = srcast(s->list.prev, ssdb, link);
		c->seq->dfsn = last->id;
		c->seq->dfsn++;
	}
	return 0;
}

int ss_open(ss *s, src *c)
{
	int exists = sr_fileexists(c->c->dir);
	if (! exists)
		return ss_create(s, c);
	return ss_recover(s, c);
}

int ss_close(ss *s, src *c)
{
	int rcret = 0;
	srlist *i, *n;
	sr_listforeach_safe(&s->list, i, n) {
		ssdb *db = srcast(i, ssdb, link);
		int rc = ss_dbclose(db, c);
		if (rc == -1)
			rcret = -1;
	}
	sr_mutexfree(&s->lockw);
	sr_spinlockfree(&s->lock);
	return rcret;
}

int ss_account(ss *s, ssdbref *r)
{
	ssdb *db = ss_match(s, r->dfsn);
	if (srunlikely(db == NULL))
		return 0;
	db->gc.mark  += r->mark;
	db->gc.sweep += r->sweep;
	return 1;
}

int ss_list(ss *s, sra *a, srbuf *list)
{
	sr_spinlock(&s->lock);
	srlist *i;
	sr_listforeach(&s->list, i) {
		ssdb *db = srcast(i, ssdb, link);
		if (sr_gcinprogress(&db->gc))
			continue;
		char *path = sr_strdup(a, db->file.file);
		if (srunlikely(path == NULL)) {
			sr_spinunlock(&s->lock);
			return -1;
		}
		int rc = sr_bufadd(list, a, &path, sizeof(char**));
		if (srunlikely(rc == -1)) {
			sr_free(a, path);
			sr_spinunlock(&s->lock);
			return -1;
		}
	}
	sr_spinunlock(&s->lock);
	return 0;
}

int ss_listref(ss *s, sra *a, srbuf *list)
{
	int start = sr_bufused(list);
	int rc = sr_bufensure(list, a, sizeof(uint32_t));
	if (srunlikely(rc == -1))
		return -1;
	sr_bufadvance(list, sizeof(uint32_t));

	sr_spinlock(&s->lock);
	int n = 0;
	srlist *i;
	sr_listforeach(&s->list, i) {
		ssdb *db = srcast(i, ssdb, link);
		sr_gclock(&db->gc);
		ssdbref ref = {
			.dfsn  = db->id,
			.mark  = db->gc.mark,
			.sweep = db->gc.sweep
		};
		sr_gcunlock(&db->gc);
		int rc = sr_bufadd(list, a, &ref, sizeof(ref));
		if (srunlikely(rc == -1)) {
			sr_spinunlock(&s->lock);
			list->p = list->s + start;
			return -1;
		}
		n++;
	}
	sr_spinunlock(&s->lock);
	
	*(uint32_t*)(list->s + start) = n;
	return 0;
}

int ss_rotateready(ss *s, int wm)
{
	sr_spinlock(&s->lock);
	ssdb *db = srcast(s->list.prev, ssdb, link);
	int ready = sr_gcrotateready(&db->gc, wm);
	sr_spinunlock(&s->lock);
	return ready;
}

int ss_rotate(ss *s, src *c)
{
	ssdb *db = ss_dbnew(c);
	if (srunlikely(db == NULL))
		return -1;
	ssdb *prev = NULL;
	sr_spinlock(&s->lock);
	if (s->n) {
		prev = srcast(s->list.prev, ssdb, link);
		sr_gccomplete(&prev->gc);
		ss_dbref(prev);
	}
	ss_attach(s, db);
	sr_spinunlock(&s->lock);
	if (prev) {
		int rc = sr_filesync(&prev->file);
		if (srunlikely(rc == -1))
			return -1;
		ss_dbunref(prev);
	}
	return 0;
}

ssdb*
ss_match(ss *s, uint32_t dfsn)
{
	assert(s->n > 0);
	ssdb *match = NULL;
	srlist *i;
	sr_listforeach(&s->list, i) {
		ssdb *db = srcast(i, ssdb, link);
		if (db->id == dfsn) {
			match = db;
			break;
		}
	}
	return match;
}

int ss_read(ss *s, src *c, srbuf *rbuf, sspage *page, ssref *p, ssdb **dbp)
{
	sr_bufreset(rbuf);
	int rc = sr_bufensure(rbuf, c->a, p->size);
	if (srunlikely(rc == -1))
		return -1;

	sr_spinlock(&s->lock);
	ssdb *db = ss_match(s, p->dfsn);
	assert(db != NULL);
	ss_dbref(db);
	sr_spinunlock(&s->lock);

	rc = sr_filepread(&db->file, p->offset, rbuf->s, p->size);
	ss_pageinit(page, &db->file, (sspageheader*)rbuf->s);
	ss_dbunref(db);
	if (dbp)
		*dbp = db;
	return rc;
}

int ss_writeinit(sswrite *w, int sync, ssindex *i, sspagebuild *b,
                 srbuf *buf, int count)
{
	w->sync   = sync;
	w->lock   = 1;
	w->dfsn   = 0;
	w->offset = 0;
	w->i      = i;
	w->b      = b;
	w->buf    = buf;
	w->count  = count;
	w->cb     = NULL;
	w->cbarg  = NULL;
	return 0;
}

int ss_writeinit_callback(sswrite *w, sswritef cb, void *cbarg)
{
	w->cb    = cb;
	w->cbarg = cbarg;
	return 0;
}

int ss_write(ss *s, sswrite *w)
{
	assert(s->n > 0);
	sr_spinlock(&s->lock);
	ssdb *db = srcast(s->list.prev, ssdb, link);
	ss_dbref(db);
	sr_spinunlock(&s->lock);

	if (srlikely(w->lock))
		ss_lock(s);
	int count;
	int rc;
	w->dfsn = db->id;
	if (w->b) {
		/* write pagebuild */
		count = ss_pagebuild_npage(w->b);
		rc = ss_dbwrite(db, w->b, &w->offset);
	} else {
		/* write buffer */
		count = w->count;
		rc = ss_dbwritebuf(db, w->buf, w->count, &w->offset);
	}
	if (srunlikely(rc == -1))
		goto error;
	if (w->i)
		ss_indexrebase(w->i, db->id, w->offset);
	if (w->cb) {
		rc = w->cb(w);
		if (srunlikely(rc == -1))
			goto error;
	}
	sr_spinlock(&s->lock);
	s->nwrite += count;
	sr_spinunlock(&s->lock);
	if (srlikely(w->lock))
		ss_unlock(s);

	if (w->sync)
		rc = sr_filesync(&db->file);
	ss_dbunref(db);
	return rc;

error:
	if (srlikely(w->lock))
		ss_unlock(s);
	ss_dbunref(db);
	return -1;
}

int ss_written(ss *s)
{
	sr_spinlock(&s->lock);
	int n = s->nwrite;
	sr_spinunlock(&s->lock);
	return n;
}

int ss_dropindex(ss *s, ssindex *index)
{
	sriter i;
	sr_iterinit(&i, &ss_indexiterraw, NULL);
	sr_iteropen(&i, index);
	sr_spinlock(&s->lock);
	ssdb *db = NULL;
	while (sr_iterhas(&i))
	{
		ssref *p = sr_iterof(&i);
		if (db == NULL || db->id != p->dfsn)
			db = ss_match(s, p->dfsn);
		assert(db != NULL);
		sr_gcsweep(&db->gc, 1);
		sr_iternext(&i);
	}
	sr_spinunlock(&s->lock);
	return 0;
}

int ss_drop(ss *s, sspagebuild *b, uint32_t dsn)
{
	int rc = ss_pagebuild_drop(b, dsn);
	if (srunlikely(rc == -1))
		return -1;
	sswrite w;
	ss_writeinit(&w, 1, NULL, b, NULL, 0);
	return ss_write(s, &w);
}
