#ifndef SD_INDEX_H_
#define SD_INDEX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdindex sdindex;

struct sdindex {
	srspinlock lock;
	sdnodeindex i;
	srlist link;
};

int sd_indexinit(sdindex*, src*);
int sd_indexisinit(sdindex*);
int sd_indexprepare(sdindex*, src*);
int sd_indexfree(sdindex*, src*);
int sd_indexdrop(sdindex*, ss*);
int sd_indexsnapshot(sdindex*, ssc*);

static inline void
sd_indexlock(sdindex *i) {
	sr_spinlock(&i->lock);
}

static inline void
sd_indexunlock(sdindex *i) {
	sr_spinunlock(&i->lock);
}

#endif
