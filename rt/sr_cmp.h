#ifndef SR_CMP_H_
#define SR_CMP_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef enum {
	SR_CMPU32,
	SR_CMPSTRING,
	SR_CMPCUSTOM
} srcmptype;

typedef int (*srcmpf)(char *a, size_t asz, char *b, size_t bsz, void *arg);

typedef struct srcomparator srcomparator;

struct srcomparator {
	char *name;
	srcmptype type;
	srcmpf cmp;
	void *cmparg;
	srlist link;
};

typedef struct srcmpindex srcmpindex;

struct srcmpindex {
	srlist i;
};

int sr_cmpindex_init(srcmpindex*, sra*);
int sr_cmpindex_free(srcmpindex*, sra*);
int sr_cmpindex_add(srcmpindex*, sra*, srcmptype, char*, srcmpf, void*);
srcomparator*
sr_cmpindex_match(srcmpindex*, char*);
srcomparator*
sr_cmpindex_matchtype(srcmpindex*, srcmptype);

static inline int
sr_compare(srcomparator *c, char *a, size_t asize, char *b, size_t bsize)
{
	return c->cmp(a, asize, b, bsize, c->cmparg);
}

#endif
