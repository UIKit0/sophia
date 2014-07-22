
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsl.h>
#include <libss.h>
#include <libsm.h>
#include <libsd.h>
#include <libse.h>
#include <sophia.h>

static inline seprocess*
se_processnew(src *c, srthreadf f, void *arg)
{
	seprocess *p = sr_malloc(c->a, sizeof(seprocess));
	if (srunlikely(p == NULL))
		return NULL;
	p->arg = arg;
	ss_cinit(&p->sc, c);
	sr_listinit(&p->link);
	int rc = sr_threadnew(&p->t, f, p);
	if (srunlikely(rc == -1)) {
		sr_free(c->a, p);
		return NULL;
	}
	return p;
}

static inline int
se_processshutdown(seprocess *p, src *c)
{
	sr_threadwakeup(&p->t);
	int rc = sr_threadjoin(&p->t);
	ss_cfree(&p->sc, c);
	sr_free(c->a, p);
	return rc;
}

int se_processmgr_init(seprocessmgr *m)
{
	sr_listinit(&m->list);
	m->n = 0;
	return 0;
}

int se_processmgr_shutdown(seprocessmgr *m, src *c)
{
	int rcret = 0;
	int rc;
	srlist *i, *n;
	sr_listforeach_safe(&m->list, i, n) {
		seprocess *p = srcast(i, seprocess, link);
		rc = se_processshutdown(p, c);
		if (srunlikely(rc == -1))
			rcret = -1;
	}
	return rcret;
}

int se_processmgr_new(seprocessmgr *m, src *c, int n, srthreadf f, void *arg)
{
	int i = 0;
	while (i < n) {
		seprocess *p = se_processnew(c, f, arg);
		if (srunlikely(p == NULL))
			return -1;
		sr_listappend(&m->list, &p->link);
		m->n++;
		i++;
	}
	return 0;
}

int se_processmgr_wakeup(seprocessmgr *m)
{
	srlist *i;
	sr_listforeach(&m->list, i) {
		seprocess *p = srcast(i, seprocess, link);
		sr_threadwakeup(&p->t);
	}
	return 0;
}
