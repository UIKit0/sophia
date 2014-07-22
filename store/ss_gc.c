
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libss.h>

int ss_gcschedule(ss *s, sra *a, float factor, ssdblist **list)
{
	ssdblist *gcl = NULL;
	int count = 0;
	*list = NULL;
	sr_spinlock(&s->lock);
	srlist *i;
	sr_listforeach(&s->list, i) {
		ssdb *db = srcast(i, ssdb, link);
		if (srlikely(! sr_gcready(&db->gc, factor)))
			continue;
		/*
		if (srunlikely(sr_flagstryset(&db->flags, SS_FINPLAN)))
			continue;
			*/
		if (gcl == NULL) {
			gcl = ss_dblist_new(a);
			if (srunlikely(gcl == NULL))
				goto error;
		}
		int rc = ss_dblist_add(gcl, a, db);
		if (srunlikely(rc == -1))
			goto error;
		count++;
	}
	sr_spinunlock(&s->lock);
	*list = gcl;
	return count;
error:
	if (gcl)
		ss_dblist_free(gcl, a);
	sr_spinunlock(&s->lock);
	return -1;
}

int ss_gc(ss *s, src *c)
{
	for (;;) {
		sr_spinlock(&s->lock);
		ssdb *current = NULL;
		srlist *i;
		sr_listforeach(&s->list, i) {
			ssdb *db = srcast(i, ssdb, link);
			if (srlikely(! sr_gcgarbage(&db->gc)))
				continue;
			if (srunlikely(ss_dbrefof(db) > 1))
				continue;
			ss_dbunref(db);
			sr_listunlink(&db->link);
			s->n--;
			current = db;
			break;
		}
		sr_spinunlock(&s->lock);
		if (current) {
			int rc = ss_dbgc(current, c);
			if (srunlikely(rc == -1))
				return -1;
		} else {
			break;
		}
	}
	return 0;
}
