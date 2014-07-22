#ifndef SD_PAIR_H_
#define SD_PAIR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdpair sdpair;
typedef struct sdpairquery sdpairquery;

struct sdpair {
	smindex *i;
	smindex a, b;
};

struct sdpairquery {
	sdpair *p;
	srorder order;
	src *c;
	void *key;
	int keysize;
	uint64_t lsvn;
	smv *v;
};

int sd_pairinit(sdpair*, src*);
int sd_pairfree(sdpair*, int);
int sd_pairgc(sdpair*);
int sd_pairclean(sdpair*, src*, smindex*);

static inline void
sd_paircopy(sdpair *dest, sdpair *src)
{
	*dest = *src;
	if (src->i == &src->a)
		dest->i = &dest->a;
	else
		dest->i = &dest->b;
}

static inline smindex*
sd_pairshadow(sdpair *p) {
	return (p->i == &p->a) ? &p->b : &p->a;
}

static inline smindex*
sd_pairrotate(sdpair *p)
{
	smindex *prev = p->i;
	p->i = sd_pairshadow(p);
	return prev;
}

static inline int
sd_pairreplace(sdpair *p, sra *a, sdstat *stat, uint64_t lsvn, smv *v)
{
	int rc = sm_indexreplace(p->i, a, lsvn, v);
	sm_indexstat(p->i, &stat->i);
	return rc;
}

int sd_pairquery_open(sdpairquery*, sdpair*, src*, srorder, uint64_t,
                      void*, int);
int sd_pairquery_close(sdpairquery*);
int sd_pairquery(sdpairquery*);

#endif
