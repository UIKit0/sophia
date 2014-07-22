#ifndef SM_H_
#define SM_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sm sm;
typedef struct smtx smtx;

typedef enum {
	SMUNDEF,
	SMREADY,
	SMCOMMIT,
	SMROLLBACK,
	SMWAIT,
	SMPREPARE
} smstate;

typedef smstate (*smpreparef)(smtx*, sv*, void*);

struct sm {
	sr_t tx;
	sr_i i;
	srspinlock locktx;
	srspinlock locki;
	src *c;
};

struct smtx {
	uint32_t id;
	smstate s;
	uint64_t lsvn;
	sm *m;
	svlog log;
};

static inline void
sm_lock(sm *m) {
	sr_spinlock(&m->locki);
}

static inline void
sm_unlock(sm *m) {
	sr_spinunlock(&m->locki);
}

int sm_init(sm*, src*);
int sm_free(sm*);
int sm_cmp(smtx*, uint32_t);
uint64_t sm_lsvn(sm*);

int sm_begin(sm*, smtx*);
int sm_end(smtx*);
smstate sm_prepare(smtx*, smpreparef, void*);
smstate sm_commit(smtx*);
smstate sm_rollback(smtx*);

int sm_set(smtx*, sv*);
int sm_get(smtx*, sv*, void**, int*);
int sm_match(smtx*, sv*, smv**);

smstate sm_setss(sm*, sv*, svlog*);

#endif
