#ifndef SS_PAGEBUILD_H_
#define SS_PAGEBUILD_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sspageptr sspageptr;
typedef struct sspagebuild sspagebuild;
typedef struct sspagebuildiov sspagebuildiov;

struct sspageptr {
	int k, ksize;
	int v, vsize;
};

struct sspagebuild {
	srbuf list;
	int nsnapshot;
	int ndelete;
	int ndrop;
	int n;
	int dboff;
	srbuf kbuf;
	srbuf vbuf;
	src *c;
};

struct sspagebuildiov {
	sspagebuild *b;
	int i;
	int iovmax;
};

static inline void
ss_pagebuild_init(sspagebuild *b, src *c)
{
	sr_bufinit(&b->list);
	sr_bufinit(&b->kbuf);
	sr_bufinit(&b->vbuf);
	b->dboff = 0;
	b->nsnapshot = 0;
	b->ndelete = 0;
	b->ndrop = 0;
	b->n = 0;
	b->c = c;
}

static inline void
ss_pagebuild_free(sspagebuild *b)
{
	sr_buffree(&b->list, b->c->a);
	sr_buffree(&b->kbuf, b->c->a);
	sr_buffree(&b->vbuf, b->c->a);
}

static inline void
ss_pagebuild_reset(sspagebuild *b)
{
	sr_bufreset(&b->list);
	sr_bufreset(&b->kbuf);
	sr_bufreset(&b->vbuf);
	b->dboff = 0;
	b->nsnapshot = 0;
	b->ndelete = 0;
	b->ndrop = 0;
	b->n = 0;
}

static inline sspageptr*
ss_pagebuild_ptr(sspagebuild *b) {
	return sr_bufat(&b->list, sizeof(sspageptr), b->n);
}

static inline sspageheader*
ss_pagebuild_header(sspagebuild *b) {
	return (sspageheader*)(b->kbuf.s + ss_pagebuild_ptr(b)->k);
}

static inline sspagefooter*
ss_pagebuild_footer(sspagebuild *b) {
	return (sspagefooter*)(b->vbuf.p);
}

static inline uint64_t
ss_pagebuild_offset(sspagebuild *b)
{
	sspageptr *r = ss_pagebuild_ptr(b);
	return r->k + sr_bufused(&b->vbuf) - (sr_bufused(&b->vbuf) - r->v);
}

static inline ssv*
ss_pagebuild_min(sspagebuild *b) {
	return (ssv*)((char*)ss_pagebuild_header(b) + sizeof(sspageheader));
}

static inline ssv*
ss_pagebuild_max(sspagebuild *b) {

	sspageheader *h = ss_pagebuild_header(b);
	return (ssv*)((char*)h + sizeof(sspageheader) +
	              h->sizeblock * (h->count - 1));
}

int ss_pagebuild_begin(sspagebuild*, uint32_t, uint32_t, uint16_t);
int ss_pagebuild_rollback(sspagebuild*);
int ss_pagebuild_add(sspagebuild*, sv*);
int ss_pagebuild_end(sspagebuild*);
int ss_pagebuild_write(sspagebuild*, srbuf*);

static inline void
ss_pagebuild_commit(sspagebuild *b) {
	b->n++;
}

static inline void
ss_pagebuild_iovinit(sspagebuildiov *i, sspagebuild *b, int iovmax) {
	i->b = b;
	i->iovmax = iovmax;
	i->i = 0;
}

static inline int
ss_pagebuild_iov(sspagebuildiov *i, sriov *iov) {
	int n = 0;
	while (i->i < i->b->n && n < i->iovmax) {
		sspageptr *ref =
			(sspageptr*)sr_bufat(&i->b->list, sizeof(sspageptr), i->i);
		sr_iovadd(iov, i->b->kbuf.s + ref->k, ref->ksize);
		sr_iovadd(iov, i->b->vbuf.s + ref->v, ref->vsize);
		i->i++;
		n += 2;
	}
	return i->i < i->b->n;
}

static inline int
ss_pagebuild_npage(sspagebuild *b)
{
	return b->n - b->nsnapshot - b->ndrop -
	       b->ndelete;
}

static inline ssref*
ss_pagebuild_ref(sspagebuild *b, sra *a, uint32_t psn)
{
	return ss_refalloc(a, psn,
	                   ss_pagebuild_header(b)->lsnmin,
	                   ss_pagebuild_header(b)->lsnmax,
                       ss_pagebuild_offset(b),
		               ss_pagebuild_header(b)->size +
	                       sizeof(sspageheader),
	                   ss_pagebuild_min(b)->key,
	                   ss_pagebuild_min(b)->keysize,
	                   ss_pagebuild_max(b)->key,
	                   ss_pagebuild_max(b)->keysize);
}

int ss_pagebuild_drop(sspagebuild*, uint32_t);
int ss_pagebuild_delete(sspagebuild*, uint32_t, uint32_t);

static inline sspagedb*
ss_snapshotheader(sspagebuild *b) {
	return (sspagedb*)(b->vbuf.s + b->dboff);
}

int ss_snapshot(sspagebuild*);
int ss_snapshotdb(sspagebuild*, uint32_t);
int ss_snapshotpage(sspagebuild*, ssref*);
int ss_snapshotcommit(sspagebuild*);

#endif
