
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

int sd_nodeinit(sdnode *n, src *c, uint32_t id)
{
	int rc = sd_pairinit(&n->ip, c);
	if (srunlikely(rc == -1))
		return -1;
	sr_spinlockinit(&n->lock);
	n->id = id;
	n->c = c;
	sr_flagsinit(&n->flags, 0);
	ss_indexinit(&n->index, 0);
	n->round = 0;
	return 0;
}

int sd_nodefree(sdnode *n)
{
	sd_pairfree(&n->ip, 0);
	ss_indexfree(&n->index, n->c->a);
	sr_spinlockfree(&n->lock);
	sr_flagsfree(&n->flags);
	return 0;
}

int sd_nodecmp(sdnode *n, void *key, int keysize)
{
	srcomparator *cmp = n->c->sdb->cmp;
	sd_nodelock(n);
	ssref *min = ss_indexmin(&n->index);
	ssref *max = ss_indexmax(&n->index);
	int l = sr_compare(cmp, ss_refmin(min), min->sizemin, key, keysize);
	int r = sr_compare(cmp, ss_refmax(max), max->sizemax, key, keysize);
	sd_nodeunlock(n);
	/* inside range */
	if (l <= 0 && r >= 0)
		return 0;
	/* key > range */
	if (l == -1)
		return -1;
	/* key < range */
	assert(r == 1);
	return 1;
}

int sd_nodestat(sdnode *n, sdstat *s)
{
	sm_indexstat(n->ip.i, &s->i);
	s->node = n;
	return 0;
}

int sd_nodein(sdnode *n, ssdblist *dbl)
{
	if (n->index.dfsnmin >= dbl->dfsnmin &&
	    n->index.dfsnmin <= dbl->dfsnmax)
		return 1;
	if (n->index.dfsnmax >= dbl->dfsnmin &&
	    n->index.dfsnmax <= dbl->dfsnmax)
		return 1;
	return 0;
}
