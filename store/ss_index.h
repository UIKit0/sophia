#ifndef SS_INDEX_H_
#define SS_INDEX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssindex ssindex;

struct ssindex {
	uint32_t dfsnmin;
	uint32_t dfsnmax;
	uint64_t lsnmin;
	uint64_t lsnmax;
	uint16_t keymax;
	srbuf i;
	int n;
};

static inline ssref*
ss_indexpage(ssindex *i, int pos)
{
	assert(pos >= 0 && pos < i->n);
	return *(ssref**)sr_bufat(&i->i, sizeof(ssref**), pos);
}

static inline void
ss_indexinit(ssindex *i, uint16_t keymax)
{
	i->dfsnmin = (uint32_t)-1;
	i->dfsnmax = 0; 
	i->lsnmin  = (uint64_t)-1;
	i->lsnmax  = 0;
	i->keymax  = keymax;
	sr_bufinit(&i->i);
	i->n       = 0;
}

static inline void
ss_indexfree(ssindex *i, sra *a)
{
	int pos = 0;
	while (pos < i->n) {
		ssref *p = ss_indexpage(i, pos);
		ss_refunref(p, a);
		pos++;
	}
	i->n = 0;
	sr_buffree(&i->i, a);
}

static inline void
ss_indexaccount(ssindex *i, ssref *p)
{
	if (p->lsnmin < i->lsnmin)
		i->lsnmin = p->lsnmin;
	if (p->lsnmax > i->lsnmax)
		i->lsnmax = p->lsnmax;
	if (p->dfsn < i->dfsnmin)
		i->dfsnmin = p->dfsn;
	if (p->dfsn > i->dfsnmax)
		i->dfsnmax = p->dfsn;
}

static inline int
ss_indexhas(ssindex *i) {
	return i->n > 0;
}

static inline ssref*
ss_indexmin(ssindex *i) {
	return ss_indexpage(i, 0);
}

static inline ssref*
ss_indexmax(ssindex *i) {
	return ss_indexpage(i, i->n - 1);
}

int ss_indexadd(ssindex*, sra*, ssref*);
int ss_indexrebase(ssindex*, uint32_t, uint64_t);
int ss_indexcopy(ssindex*, sra*, ssindex*, int, int);

#endif
