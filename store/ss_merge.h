#ifndef SS_MERGE_H_
#define SS_MERGE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssmergegc ssmergegc;
typedef struct ssmerge ssmerge;

struct ssmergegc {
	ssdb *db;
	int sweep;
};

struct ssmerge {
	ss *s;
	ssindex result;
	ssindex *index;
	sriter *stream;
	uint16_t keysize;
	uint32_t dsn;
	uint64_t lsvn;
	sspagebuild *b;
	srbuf *rbuf;
	srbuf *gc;
	src *c;
};

int ss_mergeinit(ssmerge*, ss*, src*, ssc*, uint32_t, ssindex*,
                 sriter*, uint32_t,
                 uint64_t);
int ss_mergefree(ssmerge*);
int ss_mergewrite(ssmerge*, sswritef, void*);
int ss_merge(ssmerge*);

#endif
