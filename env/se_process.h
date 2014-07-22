#ifndef SE_PROCESS_H_
#define SE_PROCESS_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct seprocessmgr seprocessmgr;
typedef struct seprocess seprocess;

struct seprocess {
	srthread t;
	void *arg;
	ssc sc;
	srlist link;
};

struct seprocessmgr {
	srlist list;
	int n;
};

int se_processmgr_init(seprocessmgr*);
int se_processmgr_shutdown(seprocessmgr*, src*);
int se_processmgr_new(seprocessmgr*, src*, int, srthreadf, void*);
int se_processmgr_wakeup(seprocessmgr*);

static inline void
se_processwait(seprocess *p) {
	sr_threadwait(&p->t);
}

static inline void
se_processwait_timeout(seprocess *p, int secs) {
	sr_threadwait_tm(&p->t, secs);
}

static inline void
se_processstub_init(seprocess *p, src *c, void *arg)
{
	memset(&p->t, 0, sizeof(p->t));
	p->arg = arg;
	ss_cinit(&p->sc, c);
	sr_listinit(&p->link);
}

static inline void
se_processstub_free(seprocess *p, src *c)
{
	ss_cfree(&p->sc, c);
}

#endif
