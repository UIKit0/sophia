#ifndef SD_NODE_H_
#define SD_NODE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#define SD_FINPLAN 1

typedef struct sdnode sdnode;

struct sdnode {
	srspinlock lock;
	uint32_t id;
	srflags flags;
	sdpair ip;
	ssindex index;
	src *c;
	int round;
};

static inline void
sd_nodelock(sdnode *n) {
	sr_spinlock(&n->lock);
}

static inline void
sd_nodeunlock(sdnode *n) {
	sr_spinunlock(&n->lock);
}

int sd_nodeinit(sdnode*, src*, uint32_t);
int sd_nodefree(sdnode*);
int sd_nodecmp(sdnode*, void*, int);
int sd_nodestat(sdnode*, sdstat*);
int sd_nodein(sdnode*, ssdblist*);

#endif
