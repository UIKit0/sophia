#ifndef SD_QUERY_H_
#define SD_QUERY_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdquery sdquery;

struct sdquery {
	srorder order;
	void *key;
	int keysize;
	uint64_t lsvn;
	void *value;
	int valuesize;
	sspage origin;
	sv result;
	srbuf *rbuf;
	sd *db;
	sdnode *n;
};

int sd_queryopen(sdquery*, sd*, srorder, uint64_t, void*, int);
int sd_querybuf(sdquery*, srbuf*);
int sd_queryclose(sdquery*);
int sd_query(sdquery*);

int sd_pagecommited(sd*, sv*);

#endif
