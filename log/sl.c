
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

static inline sl*
sl_alloc(slpool *p, uint32_t id)
{
	sl *l = sr_malloc(p->c->a, sizeof(*l));
	if (srunlikely(l == NULL))
		return NULL;
	l->id   = id;
	l->used = 0;
	l->p    = NULL;
	sr_gcinit(&l->gc);
	sr_spinlockinit(&l->filelock);
	sr_fileinit(&l->file, p->c->a);
	sr_listinit(&l->link);
	return l;
}

static inline int
sl_close(slpool *p, sl *l)
{
	int rc = sr_fileclose(&l->file);
	sr_spinlockfree(&l->filelock);
	sr_gcfree(&l->gc);
	sr_free(p->c->a, l);
	return rc;
}

static inline sl*
sl_open(slpool *p, uint32_t id)
{
	sl *l = sl_alloc(p, id);
	if (srunlikely(l == NULL))
		return NULL;
	srpath path;
	sr_pathA(&path, p->c->c->logdir, id, "log");
	int rc = sr_fileopen(&l->file, path.path);
	if (srunlikely(rc == -1))
		goto error;
	return l;
error:
	sl_close(p, l);
	return NULL;
}

static inline sl*
sl_new(slpool *p, uint32_t id)
{
	sl *l = sl_alloc(p, id);
	if (srunlikely(l == NULL))
		return NULL;
	srpath path;
	sr_pathA(&path, p->c->c->logdir, id, "log");
	int rc = sr_filenew(&l->file, path.path);
	if (srunlikely(rc == -1))
		goto error;
	slheader h = {
		.magic   = 0,
		.version = SL_VERSION
	};
	rc = sr_filewrite(&l->file, &h, sizeof(h));
	if (srunlikely(rc == -1))
		goto error;
	return l;
error:
	sl_close(p, l);
	return NULL;
}

int sl_poolinit(slpool *p, src *c)
{
	sr_spinlockinit(&p->lock);
	sr_listinit(&p->list);
	p->n = 0;
	p->c = c;
	p->create = 0;
	struct iovec *iov =
		sr_malloc(c->a, sizeof(struct iovec) * 1021);
	if (srunlikely(iov == NULL))
		return -1;
	sr_iovinit(&p->iov, iov, 1021);
	return 0;
}

static inline int
sl_poolcreate(slpool *p, int open)
{
	srconf *c = p->c->c;
	int rc;
	p->create = 1;
	if (! c->logdir_create)
		return -1;
	if (! c->logdir_write)
		return -1;
	if (open && c->create_on_write)
		return 0;
	rc = sr_filemkdir(c->logdir);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

static inline int
sl_poolrecover(slpool *p)
{
	srbuf list;
	sr_bufinit(&list);
	srstoretype types[] =
	{
		{ "log", 1, 0 },
		{ NULL,  0, 0 }
	};
	int rc = sr_storeread(&list, p->c->a, types, p->c->c->logdir);
	if (srunlikely(rc == -1))
		return -1;
	sriter i;
	sr_iterinit(&i, &sr_bufiter, p->c);
	sr_iteropen(&i, &list, sizeof(srstoreid));
	while(sr_iterhas(&i)) {
		srstoreid *id = sr_iterof(&i);
		sl *l = sl_open(p, id->id);
		if (srunlikely(l == NULL)) {
			sr_buffree(&list, p->c->a);
			return -1;
		}
		sr_listappend(&p->list, &l->link);
		p->n++;
		sr_iternext(&i);
	}
	sr_buffree(&list, p->c->a);
	if (p->n) {
		sl *last = srcast(p->list.prev, sl, link);
		p->c->seq->lfsn = last->id;
		p->c->seq->lfsn++;
	}
	return 0;
}

int sl_poolopen(slpool *p)
{
	int exists = sr_fileexists(p->c->c->logdir);
	int rc;
	if (! exists)
		rc = sl_poolcreate(p, 1);
	else
		rc = sl_poolrecover(p);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

int sl_poolrotate(slpool *p)
{
	uint32_t lfsn = sr_seq(p->c->seq, SR_LFSNNEXT);
	sl *l = sl_new(p, lfsn);
	if (srunlikely(l == NULL))
		return -1;
	sl *log = NULL;
	sr_spinlock(&p->lock);
	if (p->n) {
		log = srcast(p->list.prev, sl, link);
		sr_gccomplete(&log->gc);
	}
	sr_listappend(&p->list, &l->link);
	p->n++;
	sr_spinunlock(&p->lock);
	if (log) {
		int rc = sr_filesync(&log->file);
		if (srunlikely(rc == -1))
			return -1;
	}
	return 0;
}

int sl_poolrotate_ready(slpool *p, int wm)
{
	sr_spinlock(&p->lock);
	assert(p->n > 0);
	sl *l = srcast(p->list.prev, sl, link);
	int ready = sr_gcrotateready(&l->gc, wm);
	sr_spinunlock(&p->lock);
	return ready;
}

int sl_poollist(slpool *p, sra *a, srbuf *list)
{
	sr_spinlock(&p->lock);
	srlist *i;
	sr_listforeach(&p->list, i) {
		sl *l = srcast(i, sl, link);
		if (sr_gcinprogress(&l->gc))
			continue;
		char *path = sr_strdup(a, l->file.file);
		if (srunlikely(path == NULL)) {
			sr_spinunlock(&p->lock);
			return -1;
		}
		int rc = sr_bufadd(list, a, &path, sizeof(char**));
		if (srunlikely(rc == -1)) {
			sr_free(a, path);
			sr_spinunlock(&p->lock);
			return -1;
		}
	}
	sr_spinunlock(&p->lock);
	return 0;
}

int sl_poolshutdown(slpool *p)
{
	int rcret = 0;
	int rc;
	srlist *i, *n;
	sr_listforeach_safe(&p->list, i, n) {
		sl *l = srcast(i, sl, link);
		rc = sl_close(p, l);
		if (srunlikely(rc == -1))
			rcret = -1;
	}
	if (p->iov.v)
		sr_free(p->c->a, p->iov.v);
	sr_spinlockfree(&p->lock);
	return rcret;
}

static inline int
sl_gc(slpool *p, sl *l)
{
	int rc;
	rc = sr_fileunlink(l->file.file);
	if (srunlikely(rc == -1))
		return -1;
	rc = sl_close(p, l);
	if (srunlikely(rc == -1))
		return -1;
	return 1;
}

int sl_poolgc(slpool *p)
{
	for (;;) {
		sr_spinlock(&p->lock);
		sl *current = NULL;
		srlist *i;
		sr_listforeach(&p->list, i) {
			sl *l = srcast(i, sl, link);
			if (srlikely(! sr_gcgarbage(&l->gc)))
				continue;
			sr_listunlink(&l->link);
			p->n--;
			current = l;
			break;
		}
		sr_spinunlock(&p->lock);
		if (current) {
			int rc = sl_gc(p, current);
			if (srunlikely(rc == -1))
				return -1;
		} else {
			break;
		}
	}
	return 0;
}

int sl_begin(slpool *p, sltx *t)
{
	sr_spinlock(&p->lock);
	assert(p->n > 0);
	sl *l = srcast(p->list.prev, sl, link);
	sr_spinlock(&l->filelock);
	t->svp = sr_filesvp(&l->file);
	t->p = p;
	t->l = l;
	return 0;
}

int sl_commit(sltx *t)
{
	sr_spinunlock(&t->l->filelock);
	sr_spinunlock(&t->p->lock);
	return 0;
}

int sl_rollback(sltx *t)
{
	int rc = sr_filerlb(&t->l->file, t->svp);
	sr_spinunlock(&t->l->filelock);
	sr_spinunlock(&t->p->lock);
	return rc;
}

static inline int
sl_create_on_write(slpool *p)
{
	if (srunlikely(p->create && p->c->c->create_on_write))
	{
		p->create = 0;
		return sl_poolcreate(p, 0);
	}
	return 0;
}

int sl_write(sltx *t, svlog *vlog, uint32_t dsn)
{
	slpool *p = t->p;
	sl *l = t->l;
	int rc = sl_create_on_write(p);
	if (srunlikely(rc == -1))
		return -1;

	uint64_t lsn = sr_seq(p->c->seq, SR_LSNNEXT);

	slv lvbuf[341]; /* 1 + 340 per syscall */
	int lvp;
	lvp = 0;

	/* begin */
	slv *lv = &lvbuf[0];
	lv->lsn       = lsn;
	lv->dsn       = dsn;
	lv->flags     = SVBEGIN;
	lv->valuesize = sv_logn(vlog);
	lv->keysize   = 0;
	lv->crc       = sr_crcs(lv, sizeof(slv), 0);
	sr_iovadd(&p->iov, lv, sizeof(slv));
	lvp++;

	sriter i;
	sr_iterinit(&i, &sr_bufiter, p->c);
	sr_iteropen(&i, &vlog->buf, sizeof(sv));

	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *v = sr_iterof(&i);
		svlsnset(v, lsn);

		if (srunlikely(! sr_iovensure(&p->iov, 3))) {
			int rc = sr_filewritev(&l->file, &p->iov);
			if (srunlikely(rc == -1))
				return -1;
			sr_iovreset(&p->iov);
			lvp = 0;
		}
		/* prepare header */
		lv = &lvbuf[lvp];
		lv->lsn       = lsn;
		lv->dsn       = dsn;
		lv->flags     = svflags(v);
		lv->valuesize = svvaluesize(v);
		lv->keysize   = svkeysize(v);
		lv->crc       = sr_crcp(svkey(v), lv->keysize, 0);
		lv->crc       = sr_crcp(svvalue(v), lv->valuesize, lv->crc);
		lv->crc       = sr_crcs(lv, sizeof(slv), lv->crc);
		/* prepare to write */
		sr_iovadd(&p->iov, lv, sizeof(slv));
		sr_iovadd(&p->iov, svkey(v), lv->keysize);
		sr_iovadd(&p->iov, svvalue(v), lv->valuesize);
		lvp++;
	}

	if (srlikely(sr_iovhas(&p->iov))) {
		int rc = sr_filewritev(&l->file, &p->iov);
		if (srunlikely(rc == -1))
			return -1;
		sr_iovreset(&p->iov);
	}
	sr_gcmark(&l->gc, sv_logn(vlog));
	return 0;
}
