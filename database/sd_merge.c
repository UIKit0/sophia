
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

typedef struct sdmerge sdmerge;

struct sdmerge {
	int splitted;
	sdsplit split;
	sdpair splitip;
	sd *db;
	sdnode *node;
	smindex *index;
	ssmerge m;
};

static inline int
sd_mergecommit_split(sdmerge *merge)
{
	sd *db = merge->db;
	sdnode *node = merge->node;
	ssmerge *m = &merge->m;
	sdsplit *s = &merge->split;

	/* split case.
	 * create new nodes from the page index
	*/
	merge->splitted = 1;
	sd_splitinit(&merge->split, db->c, &m->result);
	int rc = sd_split(s);
	if (srunlikely(rc == -1))
		return -1;
	/* redistribute key index among newly created
	 * nodes and commit changes */
	sd_indexlock(&db->primary);
	sd_nodelock(node);
	rc = sd_splitrange(s, node->ip.i);
	if (srunlikely(rc == -1))
		goto error;
	rc = sd_nodeindex_update(&db->primary.i, db->c->a, node,
	                         s->count, s->n);
	if (srunlikely(rc == -1))
		goto error;
	sd_paircopy(&merge->splitip, &node->ip);
	sd_paircopy(&node->ip, &s->iporigin);
	node->index = s->iorigin;
error:
	sd_nodeunlock(node);
	sd_indexunlock(&db->primary);
	return rc;
}

static int
sd_mergecommit(sswrite *w)
{
	sdmerge *merge = w->cbarg;
	if (srlikely(merge->m.result.n >= (merge->db->c->c->node_size * 2)))
		return sd_mergecommit_split(merge);

	/* no split case.
	 * update node key index and free old data
	*/
	ssmerge *m = &merge->m;
	sd *db = merge->db;
	sdnode *node = merge->node;

	/* apply new index.
	 * might delete node if it's index become empty
	*/
	sd_indexlock(&db->primary);
	sd_nodelock(node);
	int rc = 0;
	if (m->result.n) {
		node->index = m->result;
	} else {
		/* empty index must contain a single
		 * empty node */
		if (db->primary.i.n <= 1) {
			node->index = m->result;
			ss_indexinit(&node->index, 0);
		} else {
			rc = sd_nodeindex_delete(&db->primary.i, db->c->a, node);
		}
	}
	sd_nodeunlock(node);
	sd_indexunlock(&db->primary);
	return rc;
}

int sd_merge(sd *db, sdnode *node, ssc *sc, uint64_t lsvn)
{
	sdmerge merge;
	memset(&merge, 0, sizeof(merge));
	merge.db   = db;
	merge.node = node;

	/* rotate node key index */
	sd_nodelock(node);
	merge.index = sd_pairrotate(&node->ip);
	sd_nodeunlock(node);
	ssindex oldindex = node->index;

	/* create new page index, by merging key index
	 * with current page index */
	sriter i;
	sr_iterinit(&i, &sm_indexiterraw, db->c);
	sr_iteropen(&i, merge.index);
	ss_mergeinit(&merge.m, db->store, db->c, sc, db->scheme->dsn,
	             &node->index,
	             &i, merge.index->keymax, lsvn);
	int rc = ss_merge(&merge.m);
	if (srunlikely(rc == -1)) {
		sd_splitfree(&merge.split);
		ss_mergefree(&merge.m);
		return -1;
	}
	rc = ss_mergewrite(&merge.m, sd_mergecommit, &merge);
	if (srunlikely(rc == -1)) {
		sd_splitfree(&merge.split);
		ss_mergefree(&merge.m);
		return -1;
	}

	/* cleanup */
	ss_indexfree(&oldindex, db->c->a);
	if (! merge.splitted)
	{
		sd_nodelock(node);
		rc = sd_pairclean(&node->ip, db->c, merge.index);
		sd_nodeunlock(node);
	} else {
		ss_mergefree(&merge.m);
		sd_splitgc(&merge.split);
		sd_pairgc(&merge.splitip);
	}
	return rc;

}
