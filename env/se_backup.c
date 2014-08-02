
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

static inline void
se_backupdestroy_list(srbuf *list, sra *a)
{
	sriter i;
	sr_iterinit(&i, &sr_bufiterref, NULL);
	sr_iteropen(&i, list, sizeof(char**));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		char *path = sr_iterof(&i);
		sr_free(a, path);
	}
	sr_buffree(list, a);
}

static int
se_backupbegin(sebackup *b)
{
	int rc = sl_poolrotate(&b->e->lp);
	if (srunlikely(rc == -1))
		return -1;
	rc = ss_rotate(&b->e->store, &b->e->c);
	if (srunlikely(rc == -1))
		return -1;
	rc = sl_poollist(&b->e->lp, &b->e->a, &b->list);
	if (srunlikely(rc == -1))
		return -1;
	rc = ss_list(&b->e->store, &b->e->a, &b->list);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

static int
se_backupdestroy(seobj *o)
{
	sebackup *b = (sebackup*)o;
	se *e = b->e;
	se_backupdestroy_list(&b->list, &e->a);
	/* object destroy automatically enables gc */
	se_objindex_unregister(&e->oi, o);
	sr_free(&e->a, o);
	return 0;
}

static int
se_backupfetch(seobj *o)
{
	sebackup *b = (sebackup*)o;
	if (srunlikely(b->v == NULL))
		return 0;
	if (srunlikely(b->ready)) {
		b->ready = 0;
		return 1;
	}
	b->v++;
	if (srunlikely(! sr_bufin(&b->list, (char*)b->v))) {
		b->v = NULL;
		return 0;
	}
	return 1;
}

static void*
se_backupkey(seobj *o)
{
	sebackup *b = (sebackup*)o;
	if (srunlikely(b->v == NULL))
		return NULL;
	return *b->v;
}

static size_t
se_backupkeysize(seobj *o)
{
	sebackup *b = (sebackup*)o;
	if (srunlikely(b->v == NULL))
		return 0;
	return strlen(*b->v);
}

static seobjif sebackupif =
{
	.ctl       = NULL,
	.database  = NULL,
	.open      = NULL,
	.destroy   = se_backupdestroy, 
	.set       = NULL,
	.get       = NULL,
	.del       = NULL,
	.begin     = NULL,
	.commit    = NULL,
	.rollback  = NULL,
	.cursor    = NULL,
	.fetch     = se_backupfetch,
	.key       = se_backupkey,
	.keysize   = se_backupkeysize,
	.value     = NULL,
	.valuesize = NULL, 
	.backup    = NULL
};

seobj *se_backupnew(se *e)
{
	if (srunlikely(! se_active(e)))
		return NULL;
	sebackup *b = sr_malloc(&e->a, sizeof(sebackup));
	if (srunlikely(b == NULL))
		return NULL;
	se_objinit(&b->o, SEOBACKUP, &sebackupif);
	b->ready = 0;
	b->e = e;
	b->v = NULL;
	sr_bufinit(&b->list);
	se_objindex_register(&e->oi, &b->o);
	int rc = se_backupbegin(b);
	if (srunlikely(rc == -1)) {
		se_backupdestroy(&b->o);
		return NULL;
	}
	assert(sr_bufused(&b->list) > 0);
	b->v = (char**)b->list.s;
	b->ready = 1;
	return &b->o;
}
