
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

ssdb *ss_dballoc(src *c, uint32_t dfsn)
{
	ssdb *db = sr_malloc(c->a, sizeof(ssdb));
	if (srunlikely(db == NULL))
		return NULL;
	db->id  = dfsn;
	db->ref = 0;
	sr_spinlockinit(&db->lock);
	sr_gcinit(&db->gc);
	sr_fileinit(&db->file, c->a);
	sr_listinit(&db->link);
	sr_flagsinit(&db->flags, 0);
	return db;
}

ssdb *ss_dbnew(src *c)
{
	uint32_t dfsn = sr_seq(c->seq, SR_DFSNNEXT);
	ssdb *db = ss_dballoc(c, dfsn);
	if (srunlikely(db == NULL))
		return NULL;
	srpath path;
	sr_pathA(&path, c->c->dir, dfsn, "db");
	int rc = sr_filenew(&db->file, path.path);
	if (srunlikely(rc == -1)) {
		ss_dbclose(db, c);
		return NULL;
	}
	return db;
}

static inline int
ss_dbrecover(ssdb *db)
{
	if (srunlikely(db->file.size == 0))
		return 0;
	srmap map;
	int rc = sr_map(&map, db->file.fd, db->file.size, 1);
	if (srunlikely(rc == -1))
		return -1;
	void *ptr = (char*)map.p + map.size - sizeof(sspagefooter);
	sspagefooter *f = (sspagefooter*)ptr;
	if (srlikely(f->marker == SS_PAGEMARKER)) {
		sr_mapunmap(&map);
		return 0;
	}
	/* TODO match footer and truncate */
	assert(0);
	sr_mapunmap(&map);
	return 0;
}

ssdb *ss_dbopen(src *c, uint32_t dfsn)
{
	ssdb *db = ss_dballoc(c, dfsn);
	if (srunlikely(db == NULL))
		return NULL;
	srpath path;
	sr_pathA(&path, c->c->dir, dfsn, "db");
	int rc = sr_fileopen(&db->file, path.path);
	if (srunlikely(rc == -1))
		goto error;
	rc = sr_fileseek(&db->file, db->file.size);
	if (srunlikely(rc == -1))
		goto error;
	rc = ss_dbrecover(db);
	if (srunlikely(rc == -1))
		goto error;
	return db;
error:
	ss_dbclose(db, c);
	return NULL;
}

int ss_dbclose(ssdb *db, src *c)
{
	int rcret = 0;
	int rc = sr_fileclose(&db->file);
	if (srunlikely(rc == -1))
		rcret = -1;
	sr_gcfree(&db->gc);
	sr_flagsfree(&db->flags);
	sr_spinlockfree(&db->lock);
	sr_free(c->a, db);
	return rcret;
}

int ss_dbgc(ssdb *db, src *c)
{
	int rc;
	rc = sr_fileunlink(db->file.file);
	if (srunlikely(rc == -1))
		return -1;
	return ss_dbclose(db, c);
}

int ss_dbwrite(ssdb *db, sspagebuild *b, uint64_t *offset)
{
	*offset = sr_filesvp(&db->file);
	if (srunlikely(b->n == 0))
		return 0;
	struct iovec iovv[1024];
	sriov iov;
	sr_iovinit(&iov, iovv, 1024);
	sspagebuildiov iter;
	ss_pagebuild_iovinit(&iter, b, 1024);
	ss_pagebuild_iov(&iter, &iov);
	while (sr_iovhas(&iov)) {
		int rc = sr_filewritev(&db->file, &iov);
		if (srunlikely(rc == -1))
			goto rlb;
		sr_iovinit(&iov, iovv, 1024);
		ss_pagebuild_iov(&iter, &iov);
	}
	int mark = ss_pagebuild_npage(b);
	sr_gcmark(&db->gc, mark);
	return 0;
rlb:
	sr_filerlb(&db->file, *offset);
	return -1;
}

int ss_dbwritebuf(ssdb *db, srbuf *b, int count, uint64_t *offset)
{
	*offset = sr_filesvp(&db->file);
	int rc = sr_filewrite(&db->file, b->s, sr_bufused(b));
	if (srunlikely(rc == -1))
		goto rlb;
	sr_gcmark(&db->gc, count);
	return 0;
rlb:
	sr_filerlb(&db->file, *offset);
	return -1;
}
