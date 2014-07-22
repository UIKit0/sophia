#ifndef SS_PAGE_H_
#define SS_PAGE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sspageheader sspageheader;
typedef struct sspagefooter sspagefooter;
typedef struct sspage sspage;
typedef struct sspagedb sspagedb;

#define SS_PAGEDELETE   1
#define SS_PAGEDROP     2
#define SS_PAGESNAPSHOT 4

struct sspageheader {
	uint32_t crc;
	uint8_t  flags;
	uint32_t psn;
	uint32_t dsn;
	uint16_t count;
	uint16_t sizeblock;
	uint32_t size;
	uint64_t lsnmin;
	uint64_t lsnmax;
} srpacked;

#define SS_PAGEMARKER 0x70F1A70F1A70F1A0ULL

struct sspagefooter {
	uint32_t size;
	uint64_t marker;
} srpacked;

struct sspagedb {
	uint32_t dsn;
	uint32_t count;
} srpacked;

struct sspage {
	sspageheader *h;
	srfile *f;
};

static inline void
ss_pageinit(sspage *p, srfile *f, sspageheader *h)
{
	p->h = h;
	p->f = f;
}

static inline ssv*
ss_pagev(sspage *p, int pos) {
	assert(p->h->flags == 0);
	assert(pos < p->h->count);
	return (ssv*)((char*)p->h + sizeof(sspageheader) +
	              p->h->sizeblock * pos);
}

static inline void*
ss_pagevalue(sspage *p, ssv *v) {
	assert((p->h->sizeblock * p->h->count) + v->valueoffset < p->h->size);
	return (char*)((char*)p->h + sizeof(sspageheader) +
	               p->h->sizeblock * p->h->count) + v->valueoffset;
}

static inline ssv*
ss_pagemin(sspage *p) {
	return ss_pagev(p, 0);
}

static inline ssv*
ss_pagemax(sspage *p) {
	return ss_pagev(p, p->h->count - 1);
}

#endif
