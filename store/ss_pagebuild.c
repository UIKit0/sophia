
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

int ss_pagebuild_begin(sspagebuild *b, uint32_t dsn, uint32_t psn, uint16_t keymax)
{
	int rc = sr_bufensure(&b->list, b->c->a, sizeof(sspageptr));
	if (srunlikely(rc == -1))
		return -1;
	sspageptr *ref =
		(sspageptr*)sr_bufat(&b->list, sizeof(sspageptr), b->n);
	ref->k     = sr_bufused(&b->kbuf);
	ref->ksize = 0;
	ref->v     = sr_bufused(&b->vbuf);
	ref->vsize = 0;
	rc = sr_bufensure(&b->kbuf, b->c->a, sizeof(sspageheader));
	if (srunlikely(rc == -1))
		return -1;
	sspageheader *h = ss_pagebuild_header(b);
	memset(h, 0, sizeof(*h));
	h->psn = psn;
	h->dsn = dsn;
	h->sizeblock = sizeof(ssv) + keymax;
	h->lsnmin = (uint64_t)-1;
	h->lsnmax = 0;
	sr_bufadvance(&b->list, sizeof(sspageptr));
	sr_bufadvance(&b->kbuf, sizeof(sspageheader));
	return 0;
}

int ss_pagebuild_rollback(sspagebuild *b)
{
	b->kbuf.p -= sizeof(sspageheader);
	b->list.p -= sizeof(sspageptr);
	return 0;
}

int ss_pagebuild_add(sspagebuild *b, sv *v)
{
	uint16_t sizeblock = ss_pagebuild_header(b)->sizeblock;

	int rc = sr_bufensure(&b->kbuf, b->c->a, sizeblock);
	if (srunlikely(rc == -1))
		return -1;

	sspageheader *h = ss_pagebuild_header(b);
	ssv *sv = (ssv*)b->kbuf.p;

	if (ss_isstore(v)) {
		memcpy(sv, svraw(v), svrawsize(v));
	} else {
		sv->lsn       = svlsn(v);
		sv->flags     = svflags(v);
		sv->valuesize = svvaluesize(v);
		sv->keysize   = svkeysize(v);
		memcpy(sv->key, svkey(v), sv->keysize);
	}

	int padding = sizeblock - sizeof(ssv) - sv->keysize;
	if (padding > 0)
		memset(sv->key + sv->keysize, 0, padding);

	rc = sr_bufensure(&b->vbuf, b->c->a, sv->valuesize);
	if (srunlikely(rc == -1))
		return -1;
	rc = svvaluecopy(v, b->vbuf.p);
	if (srunlikely(rc == -1))
		return -1;
	sv->valueoffset =
		sr_bufused(&b->vbuf) - ss_pagebuild_ptr(b)->v;
	uint32_t crc;
	crc = sr_crcp(sv->key, sv->keysize, 0);
	crc = sr_crcp(b->vbuf.p, sv->valuesize, crc);
	crc = sr_crcs(sv, sizeof(ssv), crc);
	sv->crc = crc;

	h->count++;
	h->size += sv->valuesize + sizeblock;
	if (sv->lsn > h->lsnmax)
		h->lsnmax = sv->lsn;
	if (sv->lsn < h->lsnmin)
		h->lsnmin = sv->lsn;

	sr_bufadvance(&b->kbuf, sizeblock);
	sr_bufadvance(&b->vbuf, sv->valuesize);
	return 0;
}

int ss_pagebuild_end(sspagebuild *b)
{
	sspageptr *ref = ss_pagebuild_ptr(b);
	ref->ksize = sr_bufused(&b->kbuf) - ref->k;
	ref->vsize = sr_bufused(&b->vbuf) - ref->v;

	int rc = sr_bufensure(&b->vbuf, b->c->a, sizeof(sspagefooter));
	if (srunlikely(rc == -1))
		return -1;
	sspagefooter *f = ss_pagebuild_footer(b);
	f->size   = ref->ksize + ref->vsize;
	f->marker = SS_PAGEMARKER;

	sr_bufadvance(&b->vbuf, sizeof(sspagefooter));
	ref->vsize += sizeof(sspagefooter);

	sspageheader *h = ss_pagebuild_header(b);
	h->size += sizeof(sspagefooter);
	h->crc = sr_crcs(h, sizeof(sspageheader), 0);
	return 0;
}

int ss_pagebuild_write(sspagebuild *b, srbuf *buf)
{
	int i = 0;
	while (i < b->n) {
		sspageptr *ref =
			(sspageptr*)sr_bufat(&b->list, sizeof(sspageptr), i);
		int rc = sr_bufensure(buf, b->c->a, ref->ksize + ref->vsize);
		if (srunlikely(rc == -1))
			return -1;
		memcpy(buf->p, b->kbuf.s + ref->k, ref->ksize);
		sr_bufadvance(buf, ref->ksize);
		memcpy(buf->p, b->vbuf.s + ref->v, ref->vsize);
		sr_bufadvance(buf, ref->vsize);
		i++;
	}
	return 0;
}

int ss_pagebuild_drop(sspagebuild *b, uint32_t dsn)
{
	int rc = ss_pagebuild_begin(b, dsn, 0, 0);
	if (srunlikely(rc == -1))
		return -1;
	sspageheader *h = ss_pagebuild_header(b);
	h->flags  = SS_PAGEDROP;
	h->lsnmin = 0;
	h->lsnmax = 0;
	rc = ss_pagebuild_end(b);
	if (srunlikely(rc == -1))
		return -1;
	ss_pagebuild_commit(b);
	b->ndrop++;
	return 0;
}

int ss_pagebuild_delete(sspagebuild *b, uint32_t dsn, uint32_t psn)
{
	int rc = ss_pagebuild_begin(b, dsn, psn, 0);
	if (srunlikely(rc == -1))
		return -1;
	sspageheader *h = ss_pagebuild_header(b);
	h->flags  = SS_PAGEDELETE;
	h->lsnmin = 0;
	h->lsnmax = 0;
	rc = ss_pagebuild_end(b);
	if (srunlikely(rc == -1))
		return -1;
	ss_pagebuild_commit(b);
	b->ndelete++;
	return 0;
}

int ss_snapshot(sspagebuild *b)
{
	int rc = ss_pagebuild_begin(b, 0, 0, 0);
	if (srunlikely(rc == -1))
		return -1;
	sspageheader *h = ss_pagebuild_header(b);
	h->flags  = SS_PAGESNAPSHOT;
	h->lsnmin = 0;
	h->lsnmax = 0;
	return 0;
}

int ss_snapshotdb(sspagebuild *b, uint32_t dsn)
{
	b->dboff = sr_bufused(&b->vbuf);
	int rc = sr_bufensure(&b->vbuf, b->c->a, sizeof(sspagedb));
	if (srunlikely(rc == -1))
		return -1;
	sspagedb *db = ss_snapshotheader(b);
	db->dsn   = dsn;
	db->count = 0;
	sr_bufadvance(&b->vbuf, sizeof(sspagedb));
	sspageheader *h = ss_pagebuild_header(b);
	assert(h->flags == SS_PAGESNAPSHOT);
	h->count++;
	return 0;
}

int ss_snapshotpage(sspagebuild *b, ssref *r)
{
	sspageheader *h = ss_pagebuild_header(b);
	assert(h->flags == SS_PAGESNAPSHOT);
	int size = sizeof(ssref) + r->sizemin + r->sizemax;
	int rc = sr_bufensure(&b->vbuf, b->c->a, size);
	if (srunlikely(rc == -1))
		return -1;
	memcpy(b->vbuf.p, r, sizeof(ssref));
	sr_bufadvance(&b->vbuf, sizeof(ssref));
	memcpy(b->vbuf.p, ss_refmin(r), r->sizemin);
	sr_bufadvance(&b->vbuf, r->sizemin);
	memcpy(b->vbuf.p, ss_refmax(r), r->sizemax);
	sr_bufadvance(&b->vbuf, r->sizemax);
	sspagedb *db = ss_snapshotheader(b);
	db->count++;
	return 0;
}

int ss_snapshotcommit(sspagebuild *b)
{
	int rc = ss_pagebuild_end(b);
	if (srunlikely(rc == -1))
		return -1;
	ss_pagebuild_commit(b);
	b->nsnapshot++;
	return 0;
}
