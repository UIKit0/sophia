#ifndef SL_H_
#define SL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct slheader slheader;
typedef struct sl sl;
typedef struct slpool slpool;
typedef struct sltx sltx;

#define SL_VERSION 0x01020000

struct slheader {
	uint32_t magic;
	uint32_t version;
} srpacked;

struct sl {
	uint32_t id;
	srgc gc;
	uint32_t used;
	srspinlock filelock;
	srfile file;
	slpool *p;
	srlist link;
};

struct slpool {
	int create;
	srspinlock lock;
	sriov iov;
	srlist list;
	int n;
	src *c;
};

struct sltx {
	slpool *p;
	sl *l;
	uint64_t svp;
};

int sl_poolinit(slpool*, src*);
int sl_poolopen(slpool*);
int sl_poolrotate(slpool*);
int sl_poolrotate_ready(slpool*, int);
int sl_poollist(slpool*, sra*, srbuf*);
int sl_poolshutdown(slpool*);
int sl_poolgc(slpool*);

int sl_begin(slpool*, sltx*);
int sl_commit(sltx*);
int sl_rollback(sltx*);
int sl_write(sltx*, svlog*, uint32_t);

static inline void
sl_poolstat(slpool *p, int *n)
{
	sr_spinlock(&p->lock);
	*n = p->n;
	sr_spinunlock(&p->lock);
}

#endif
