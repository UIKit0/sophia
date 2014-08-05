
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

int sd_new(sd *db, srscheme *scheme, ss *s, src *c)
{
	db->store  = s;
	db->scheme = scheme;
	db->c      = c;
	sr_bufinit(&db->rbuf);
	sr_flagsinit(&db->flags, 0);
	return sd_indexinit(&db->primary, db->c);
}

int sd_prepare(sd *db) {
	/* allocate empty node */
	sr_bufinit(&db->rbuf);
	sr_flagsinit(&db->flags, 0);
	return sd_indexprepare(&db->primary, db->c);
}

int sd_free(sd *db)
{
	int rcret = 0;
	int rc;
	rc = sd_indexfree(&db->primary, db->c);
	if (srunlikely(rc == -1))
		rcret = -1;
	sr_buffree(&db->rbuf, db->c->a);
	sr_flagsfree(&db->flags);
	return rcret;
}

int sd_recover(sd *db, sstrack *s)
{
	/* cleanup drop pages and compact index */
	ss_trackgc(s);
	ss_tracksort(s, &db->c->sdb->cmp);

	sdnode *node = sr_malloc(db->c->a, sizeof(sdnode));
	if (srunlikely(node == NULL))
		return -1;
	uint32_t nsn = sr_seq(db->c->seq, SR_NSNNEXT);
	int rc = sd_nodeinit(node, db->c, nsn);
	if (srunlikely(rc == -1)) {
		sr_free(db->c->a, node);
		return -1;
	}
	ss_indexinit(&node->index, s->keymax);
	if (srunlikely(s->size == 0)) {
		rc = sd_nodeindex_append(&db->primary.i, db->c->a, node);
		if (srunlikely(rc == -1)) {
			sd_nodefree(node);
			sr_free(db->c->a, node);
			return -1;
		}
		return 0;
	}

	int fill = db->c->c->node_size / 2;
	int i = 0;
	int n = 0;
	while (i < s->n) {
		if (n == fill) {
			rc = sd_nodeindex_append(&db->primary.i, db->c->a, node);
			if (srunlikely(rc == -1)) {
				sd_nodefree(node);
				sr_free(db->c->a, node);
				return -1;
			}
			node = sr_malloc(db->c->a, sizeof(sdnode));
			if (srunlikely(node == NULL))
				return -1;
			int rc = sd_nodeinit(node, db->c, nsn);
			if (srunlikely(rc == -1)) {
				sr_free(db->c->a, node);
				return -1;
			}
			ss_indexinit(&node->index, s->keymax);
			n = 0;
		}
		rc = ss_indexadd(&node->index, db->c->a, s->i[i]);
		if (srunlikely(rc == -1)) {
			sd_nodefree(node);
			sr_free(db->c->a, node);
			return -1;
		}
		ss_indexaccount(&node->index, s->i[i]);
		n++;
		i++;
	}
	if (n > 0) {
		rc = sd_nodeindex_append(&db->primary.i, db->c->a, node);
		if (srunlikely(rc == -1)) {
			sd_nodefree(node);
			sr_free(db->c->a, node);
			return -1;
		}
	}
	return 0;
}

int sd_drop(sd *db, ssc *sc)
{
	ss_creset(sc);
	int rc = ss_drop(db->store, &sc->build, db->scheme->dsn);
	if (srunlikely(rc == -1))
		return -1;
	sd_indexdrop(&db->primary, db->store);
	return 0;
}

int sd_snapshot(sd *db, ssc *sc)
{
	int rc = ss_snapshotdb(&sc->build, db->scheme->dsn);
	if (srunlikely(rc == -1))
		return -1;
	return sd_indexsnapshot(&db->primary, sc);
}
