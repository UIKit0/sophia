#ifndef SD_SPLIT_H_
#define SD_SPLIT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdsplit sdsplit;

struct sdsplit {
	ssindex *i;
	ssindex iorigin;
	sdpair iporigin;
	int count;
	sdnode **n;
	src *c;
};

int sd_splitinit(sdsplit*, src*, ssindex*);
int sd_splitfree(sdsplit*);
int sd_splitgc(sdsplit*);
int sd_splitrange(sdsplit*, smindex*);
int sd_split(sdsplit*);

#endif
