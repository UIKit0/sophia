#ifndef SS_REF_H_
#define SS_REF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssref ssref;

struct ssref {
	uint16_t ref;
	uint32_t psn;
	uint64_t lsnmin;
	uint64_t lsnmax;
	uint32_t size;
	uint16_t sizemin;
	uint16_t sizemax;
	uint32_t dfsn;
	uint64_t offset;
} srpacked;

static inline char*
ss_refmin(ssref *r) {
	return (char*)r + sizeof(ssref);
}

static inline char*
ss_refmax(ssref *r) {
	return (char*)r + sizeof(ssref) + r->sizemin;
}

static inline ssref*
ss_refalloc(sra *a, uint32_t psn,
            uint64_t lsnmin, uint64_t lsnmax,
            uint64_t offset, uint32_t size,
            char *min, int sizemin,
            char *max, int sizemax)
{
	ssref *r =
		(ssref*)sr_malloc(a, sizeof(ssref) + sizemin +
		                  sizemax);
	if (srunlikely(r == NULL))
		return NULL;
	r->ref     = 0;
	r->dfsn    = UINT32_MAX;
	r->psn     = psn;
	r->offset  = offset;
	r->lsnmin  = lsnmin;
	r->lsnmax  = lsnmax;
	r->size    = size;
	r->sizemin = sizemin;
	r->sizemax = sizemax;
	if (min && max) {
		memcpy(ss_refmin(r), min, sizemin);
		memcpy(ss_refmax(r), max, sizemax);
	}
	return r;
}

static inline ssref*
ss_refdup(sra *a, sspage *p, uint32_t dfsn, uint64_t offset)
{
	char *min = NULL;
	char *max = NULL;
	int minsize = 0;
	int maxsize = 0;
	if (p->h->count > 0) {
		ssv *minp = ss_pagemin(p);
		ssv *maxp = ss_pagemax(p);
		min = minp->key;
		minsize = minp->keysize;
		max = maxp->key;
		maxsize = maxp->keysize;
	}
	ssref *r =
		ss_refalloc(a, p->h->psn,
	                p->h->lsnmin,
	                p->h->lsnmax,
	                offset,
	                p->h->size + sizeof(sspageheader),
	                min,
	                minsize,
	                max,
	                maxsize);
	if (srunlikely(r == NULL))
		return NULL;
	r->dfsn = dfsn;
	return r;
}

static inline ssref*
ss_refcopy(sra *a, ssref *origin)
{
	ssref *r =
		ss_refalloc(a, origin->psn,
	                origin->lsnmin,
	                origin->lsnmax,
	                origin->offset,
	                origin->size,
	                ss_refmin(origin),
	                origin->sizemin,
	                ss_refmax(origin),
	                origin->sizemax);
	if (srunlikely(r == NULL))
		return NULL;
	r->dfsn = origin->dfsn;
	return r;
}

static inline void
ss_refref(ssref *r) {
	r->ref++;
}

static inline void
ss_refunref(ssref *r, sra *a) {
	if (srlikely(r->ref <= 1)) {
		sr_free(a, r);
	} else {
		r->ref--;
	}
}

static inline int
ss_refcmp(ssref *p, void *key, int size, srcomparator *c)
{
	register int l =
		sr_compare(c, ss_refmin(p), p->sizemin, key, size);
	register int r =
		sr_compare(c, ss_refmax(p), p->sizemax, key, size);
	/* inside page range */
	if (l <= 0 && r >= 0)
		return 0;
	/* key > page */
	if (l == -1)
		return -1;
	/* key < page */
	assert(r == 1);
	return 1;
}

static inline int
ss_refisdelete(ssref *p)
{
	return p->sizemin == 0 && p->sizemax == 0;
}

#endif
