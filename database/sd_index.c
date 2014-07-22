
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

int sd_indexinit(sdindex *i, src *c)
{
	(void)c;
	int rc = sd_nodeindex_init(&i->i);
	if (srunlikely(rc == -1))
		return -1;
	sr_listinit(&i->link);
	sr_spinlockinit(&i->lock);
	return 0;
}

int sd_indexisinit(sdindex *i)
{
	return i->i.n > 0;
}

int sd_indexprepare(sdindex *i, src *c)
{
	sdnode *n = sr_malloc(c->a, sizeof(sdnode));
	if (srunlikely(n == NULL))
		return -1;
	uint32_t nsn = sr_seq(c->seq, SR_NSNNEXT);
	int rc = sd_nodeinit(n, c, nsn);
	if (srunlikely(rc == -1)) {
		sr_free(c->a, n);
		return -1;
	}
	rc = sd_nodeindex_prepare(&i->i, c->a, n);
	if (srunlikely(rc == -1))
		goto error;
	return rc;
error:
	sd_nodefree(n);
	sr_free(c->a, n);
	return -1;
}

int sd_indexfree(sdindex *i, src *c)
{
	sr_spinlockfree(&i->lock);
	int rc = sd_nodeindex_free(&i->i, c->a);
	return rc;
}

int sd_indexdrop(sdindex *i, ss *s)
{
	int p = 0;
	while (p < i->i.n) {
		sdnode *n = sd_nodeindexv(&i->i, p);
		ss_dropindex(s, &n->index);
		p++;
	}
	return 0;
}

int sd_indexsnapshot(sdindex *i, ssc *sc)
{
	int pos = 0;
	while (pos < i->i.n)
	{
		sdnode *n = sd_nodeindexv(&i->i, pos);
		sriter it;
		sr_iterinit(&it, &ss_indexiterraw, NULL);
		sr_iteropen(&it, &n->index);
		while (sr_iterhas(&it))
		{
			ssref *p = sr_iterof(&it);
			int rc = ss_snapshotpage(&sc->build, p);
			if (srunlikely(rc == -1))
				return -1;
			sr_iternext(&it);
		}
		pos++;
	}
	return 0;
}
