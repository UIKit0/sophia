#ifndef SM_INDEX_H_
#define SM_INDEX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct smindexstat smindexstat;
typedef struct smindex smindex;

struct smindexstat {
	int n;
};

struct smindex {
	uint64_t lsnmin;
	uint64_t lsnmax;
	uint16_t keymax;
	sr_i index;
};

int sm_indexinit(smindex*, src*);
int sm_indexfree(smindex*);
int sm_indexfreei(smindex*);
int sm_indextruncate(smindex*);
int sm_indexreplace(smindex*, sra*, uint64_t, smv*);
int sm_indexstat(smindex*, smindexstat*);

static inline smv*
sm_indexmatchhead(smindex *i, void *key, int keysize) {
	return sr_iget(&i->index, key, keysize);
}

static inline smv*
sm_indexmatch(smindex *i, uint64_t lsvn, void *key, int keysize)
{
	smv *ptr = sm_indexmatchhead(i, key, keysize);
	if (srunlikely(ptr == NULL))
		return NULL;
	return sm_vland(ptr, lsvn);
}

#endif
