#ifndef SS_H_
#define SS_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ss ss;
typedef struct sswrite sswrite;

typedef int (*sswritef)(sswrite*);

struct ss {
	int create;
	srmutex lockw;
	srspinlock lock;
	srlist list;
	int n;
	int nwrite;
};

struct sswrite {
	int sync;
	int lock;
	uint32_t dfsn;
	uint64_t offset;
	ssindex *i;
	sspagebuild *b;
	srbuf *buf;
	int count;
	sswritef cb;
	void *cbarg;
};

static inline void
ss_lock(ss *s) {
	sr_mutexlock(&s->lockw);
}

static inline void
ss_unlock(ss *s) {
	sr_mutexunlock(&s->lockw);
}

ssdb *ss_match(ss*, uint32_t);

int ss_init(ss*);
int ss_open(ss*, src*);
int ss_close(ss*, src*);
int ss_account(ss*, ssdbref*);
int ss_rotateready(ss*, int);
int ss_rotate(ss*, src*);
int ss_list(ss*, sra*, srbuf*);
int ss_listref(ss*, sra*, srbuf*);

int ss_read(ss*, src*, srbuf*, sspage*, ssref*, ssdb**);
int ss_writeinit_callback(sswrite*, sswritef, void*);
int ss_writeinit(sswrite*, int, ssindex*, sspagebuild*, srbuf*, int);
int ss_write(ss*, src*, sswrite*);
int ss_written(ss*);

int ss_dropindex(ss*, ssindex*);
int ss_drop(ss*, src*, sspagebuild*, uint32_t);

#endif
