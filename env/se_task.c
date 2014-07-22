
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

void se_taskmgr_init(setaskmgr *m)
{
	sr_spinlockinit(&m->lock);
	sr_listinit(&m->wait);
	sr_listinit(&m->run);
	m->nwait = 0;
	m->nrun  = 0;
	m->ndrop = 0;
}

void se_taskmgr_free(setaskmgr *m, src *c)
{
	srlist *i, *n;
	sr_listforeach_safe(&m->wait, i, n) {
		setask *ptr = srcast(i, setask, link);
		se_taskfree(ptr, c->a);
	}
	sr_listforeach_safe(&m->run, i, n) {
		setask *ptr = srcast(i, setask, link);
		se_taskfree(ptr, c->a);
	}
	sr_spinlockfree(&m->lock);
}

void se_taskmgr_push(setaskmgr *m, setask *t)
{
	sr_spinlock(&m->lock);
	t->detached = 0;
	sr_listappend(&m->wait, &t->link);
	m->nwait++;
	if (t->c->id == SE_CMDDROP)
		m->ndrop++;;
	sr_spinunlock(&m->lock);
}

setask *se_taskmgr_pop(setaskmgr *m)
{
	sr_spinlock(&m->lock);
	if (srunlikely(m->nwait == 0)) {
		sr_spinunlock(&m->lock);
		return NULL;
	}
	srlist *l = sr_listpop(&m->wait);
	m->nwait--;
	setask *t = srcast(l, setask, link);
	sr_listinit(&t->link);
	sr_listappend(&m->run, &t->link);
	m->nrun++;
	sr_spinunlock(&m->lock);
	return t;
}

void se_taskmgr_done(setaskmgr *m, setask *t)
{
	sr_spinlock(&m->lock);
	sr_listunlink(&t->link);
	m->nrun--;
	if (t->c->id == SE_CMDDROP)
		m->ndrop--;
	sr_spinunlock(&m->lock);
}
