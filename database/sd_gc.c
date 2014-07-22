
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

typedef struct sdgc sdgc;

struct sdgc {
	sd *db;
	sdnode *node;
	sscopy copy;
};

static int sd_gccommit(sswrite *w)
{
	sdgc *gc = w->cbarg;
	sd_nodelock(gc->node);
	ss_copycommit(&gc->copy, w->dfsn, w->offset);
	sd_nodeunlock(gc->node);
	return 0;
}

int sd_gc(sd *db, sdnode *node, ssc *sc, ssdblist *gcl)
{
	sdgc gc;
	gc.db   = db;
	gc.node = node;
	int rc = ss_copyinit(&gc.copy, db->store, db->c, sc, gcl, &node->index);
	if (srunlikely(rc == -1))
		return -1;
	rc = ss_copy(&gc.copy);
	if (srunlikely(rc == -1))
		return -1;
	if (rc == 0)
		return 0;
	return ss_copywrite(&gc.copy, sd_gccommit, &gc);
}
