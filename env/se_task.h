#ifndef SE_TASK_H_
#define SE_TASK_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct setask setask;
typedef struct setaskmgr setaskmgr;

struct setask {
	void *p;
	void *db;
	sdnode *node;
	secmd *c;
	int detached;
	ssdblist *gcl;
	srlist link;
};

struct setaskmgr {
	srspinlock lock;
	srlist wait;
	srlist run;
	int nwait;
	int nrun;
	int ndrop;
};

static inline void
se_taskinit(setask *t, secmd *cmd)
{
	t->p        = NULL;
	t->db       = NULL;
	t->node     = NULL;
	t->c        = cmd;
	t->detached = 1;
	sr_listinit(&t->link);
	t->gcl      = NULL;
}

static inline setask*
se_tasknew(sra *a, secmd *cmd)
{
	setask *t = sr_malloc(a, sizeof(setask));
	if (srunlikely(t == NULL))
		return NULL;
	se_taskinit(t, cmd);
	return t;
}

static inline void
se_taskfree(setask *t, sra *a) {
	if (t->gcl)
		ss_dblist_unref(t->gcl, a);
	sr_free(a, t);
}

static inline int
se_taskrun(setask *t, void *env) {
	return t->c->f(t, env);
}

static inline int
se_taskmgr_ndrop(setaskmgr *m)
{
	sr_spinlock(&m->lock);
	int n = m->ndrop;
	sr_spinunlock(&m->lock);
	return n;
}

void se_taskmgr_init(setaskmgr*);
void se_taskmgr_free(setaskmgr*, src*);
void se_taskmgr_done(setaskmgr*, setask*);
void se_taskmgr_push(setaskmgr*, setask*);
setask *se_taskmgr_pop(setaskmgr*);

#endif
