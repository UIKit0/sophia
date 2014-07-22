
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
se_recovermetrics(se *e, uint32_t dsn, uint32_t psn,
                  uint64_t lsnmin,
                  uint64_t lsnmax)
{
	if (dsn > e->seq.dsn)
		e->seq.dsn = dsn;
	if (psn > e->seq.psn)
		e->seq.psn = psn;
	if (lsnmin > e->seq.lsn)
		e->seq.lsn = lsnmin;
	if (lsnmax > e->seq.lsn)
		e->seq.lsn = lsnmax;
}

static char*
se_recoversnapshotdb(se *e, sspagedb *db, char *ptr)
{
	seobj *odb = se_dbmatchid(e, db->dsn);
	if (srunlikely(odb == NULL)) {
		odb = se_dbprepare(e, db->dsn);
		if (srunlikely(odb == NULL))
			return NULL;
	}
	sedb *cdb = (sedb*)odb;

	uint32_t i = 0;
	while (i < db->count)
	{
		ssref *ref = (ssref*)(ptr);
		ssdb *sdb = ss_match(&e->store, ref->dfsn);
		/* page being deleted or newer version
		 * is exists (track). */
		if (srunlikely(sdb == NULL))
			goto next;
		if (ss_trackhas(&cdb->track, ref->psn)) {
			sr_gcsweep(&sdb->gc, 1);
			goto next;
		}
		ssref *ip = ss_refcopy(e->c.a, ref);
		if (srunlikely(ip == NULL))
			return NULL;
		int rc = ss_trackset(&cdb->track, ip);
		if (srunlikely(rc == -1)) {
			sr_free(e->c.a, ip);
			return NULL;
		}
		se_recovermetrics(e, db->dsn, ref->psn,
		                  ref->lsnmin,
	 	                  ref->lsnmax);
next:
		ptr += sizeof(ssref) + ref->sizemin + ref->sizemax;
		i++;
	}

	return ptr;
}

static int
se_recoversnapshot(se *e, sspage *page)
{
	assert(page->h->flags == SS_PAGESNAPSHOT);

	/* update storage files gc stats */
	char *ptr = (char*)page->h + sizeof(sspageheader);
	uint32_t n = *(uint32_t*)(ptr);
	ptr += sizeof(uint32_t);
	uint32_t i = 0;
	do {
		ss_account(&e->store, (ssdbref*)ptr);
		ptr += sizeof(ssdbref);
		i++;
	} while (i < n);

	/* read databases */
	i = 0;
	while (i < page->h->count)
	{
		sspagedb *db = (sspagedb*)ptr;
		ptr += sizeof(sspagedb);
		ptr = se_recoversnapshotdb(e, db, ptr);
		if (srunlikely(ptr == NULL))
			return -1;
		i++;
	}
	return 0;
}

static int
se_recoverstoragedb(se *e, ssdb *db)
{
	/* replay database backward */
	int snapshot = 0;
	sriter i;
	sr_iterinit(&i, &ss_dbiter, &e->c);
	int rc = sr_iteropen(&i, &db->file, SR_LT, 1);
	if (srunlikely(rc == -1))
		return -1;

	sedb *cdb = NULL;
	while (sr_iterhas(&i)) {
		sspage *p = sr_iterof(&i);

		/* recover snapshot and stop storage
		 * recover. */
		if (p->h->flags == SS_PAGESNAPSHOT) {
			rc = se_recoversnapshot(e, p);
			if (srunlikely(rc == -1))
				goto error;
			snapshot = 1;
			break;
		}

		/* match database */
		if (cdb == NULL || cdb->db.id != p->h->dsn) {
			seobj *pdb = se_dbmatchid(e, p->h->dsn);
			if (srunlikely(pdb == NULL)) {
				pdb = se_dbprepare(e, p->h->dsn);
				if (srunlikely(pdb == NULL))
					goto error;
			}
			cdb = (sedb*)pdb;
		}
		assert(cdb != NULL);
		se_recovermetrics(e, p->h->dsn, p->h->psn,
		                  p->h->lsnmin,
		                  p->h->lsnmax);

		/* database drop page.
		 *
		 * skip pages which database being
		 * dropped.
		*/
		if (srunlikely(sr_flagsisset(&cdb->db.flags, SD_FDROP)))
		{
			sr_gcmark(&db->gc, 1);
			sr_gcsweep(&db->gc, 1);
			sr_iternext(&i);
			continue;
		}
		if (srunlikely(p->h->flags == SS_PAGEDROP))
		{
			assert(sr_flagsisset(&cdb->db.flags, SD_FDROP) == 0);
			sr_flagsset(&cdb->db.flags, SD_FDROP);
			sr_iternext(&i);
			continue;
		}

		if (srunlikely(p->h->flags == 0))
			sr_gcmark(&db->gc, 1);

		/* skip if newer is found */
		if (ss_trackhas(&cdb->track, p->h->psn)) {
			sr_gcsweep(&db->gc, 1);
			sr_iternext(&i);
			continue;
		}

		/* track latest page version */
		uint64_t offset = ss_dbiter_offset(&i);
		ssref *ip = ss_refdup(e->c.a, p, db->id, offset);
		if (srunlikely(ip == NULL))
			goto error;
		rc = ss_trackset(&cdb->track, ip);
		if (srunlikely(rc == -1)) {
			sr_free(e->c.a, ip);
			goto error;
		}
		sr_iternext(&i);
	}

	/* prepare metrics for usage after
	 * recover */
	sr_seq(&e->seq, SR_DSNNEXT);
	sr_seq(&e->seq, SR_PSNNEXT);
	return snapshot;
error:
	sr_iterclose(&i);
	return -1;
}

static inline int
se_recoverstorage(se *e)
{
	int snapshot = 0;
	ssdb *last = NULL;
	srlist *i;
	sr_listforeach_reverse(&e->store.list, i) {
		ssdb *db = srcast(i, ssdb, link);
		if (snapshot == 0) {
			int rc = se_recoverstoragedb(e, db);
			if (srunlikely(rc == -1))
				return -1;
			if (rc)
				snapshot = 1;
		}
		if (last)
			sr_gccomplete(&db->gc);
		last = db;
	}
	return 0;
}

static inline int
se_recoverdb(sedb *db)
{
	int rc = sd_recover(&db->db, &db->track);
	if (srunlikely(rc == -1))
		return -1;
	ss_trackfreeindex(&db->track);
	return 0;
}

static inline int
se_recoverscheme(se *e)
{
	/* recover scheme first */
	sedb *scheme = ((sescheme*)(e->objscheme))->scheme;
	int rc = se_recoverdb(scheme);
	if (srunlikely(rc == -1))
		return -1;

	/* recreate database schemas */
	sr_seq(&e->seq, SR_LSNNEXT);

	void *sc = sp_cursor(scheme, ">", NULL, 0);
	if (srunlikely(sc == NULL))
		return -1;
	while (sp_fetch(sc))
	{
		srscheme s;
		int rc = sr_schemeinit_from(&s, &e->a, &e->ci,
		                            sp_value(sc),
		                            sp_valuesize(sc));
		if (srunlikely(rc == -1)) {
			sp_destroy(sc);
			return -1;
		}
		sedb *db = (sedb*)se_dbmatchid(e, s.dsn);
		if (srunlikely(db == NULL)) {
			db = (sedb*)se_dbprepare(e, s.dsn);
			if (srunlikely(db == NULL)) {
				sr_schemefree(&s, &e->a);
				sp_destroy(sc);
				return -1;
			}
		}
		se_dbdeploy(db, &s);
	}
	sp_destroy(sc);

	e->seq.lsn--;

	/* ensure all scheme are recovered */
	srlist *i;
	sr_listforeach(&e->oi.dblist, i) {
		sedb *db = srcast(i, sedb, o.olink);
		if (db->db.id == 0)
			continue;
		if (db->scheme.undef)
			return -1;
	}

	return 0;
}

static inline int
se_recoverbuild(se *e)
{
	srlist *i, *n;
	sr_listforeach_safe(&e->oi.dblist, i, n) {
		sedb *db = srcast(i, sedb, o.olink);
		if (db->db.id == 0)
			continue;
		int rc;
		if (srunlikely(sr_flagsisset(&db->db.flags, SD_FDROP)))
			rc = se_dbdrop(db);
		else
			rc = se_recoverdb(db);
		if (srunlikely(rc == -1))
			return -1;
	}
	return 0;
}

static inline int
se_recoverlog(se *e, sl *log)
{
	void *tx = NULL;
	sriter i;
	sr_iterinit(&i, &sl_iter, &e->c);
	int rc = sr_iteropen(&i, &log->file, 1);
	if (srunlikely(rc == -1))
		return -1;
	for (;;)
	{
		sv *v = sr_iterof(&i);
		if (srunlikely(v == NULL))
			break;
		uint32_t dsn = svdsn(v);
		uint64_t lsn = svlsn(v);
		if (lsn > e->seq.lsn)
			e->seq.lsn = lsn;

		sedb *dbsrc;
		void *db;
		if (srunlikely(dsn == 0)) {
			db = e->objscheme;
			dbsrc = ((sescheme*)e->objscheme)->scheme;
		} else {
			db = se_dbmatchid(e, dsn);
			if (srunlikely(db == NULL))
				goto error;
			dbsrc = db;
		}

		/* ensure that this update was not previously
		 * merged to disk */
		if (sd_pagecommited(&dbsrc->db, v))
		{
			/* skip transaction */
			while (sr_iterhas(&i)) {
				sr_gcmark(&log->gc, 1);
				sr_gcsweep(&log->gc, 1);
				sr_iternext(&i);
			}
			if (srunlikely(sl_itererror(&i)))
				goto error;
			if (! sl_itercontinue(&i) )
				break;
			continue;
		}

		tx = sp_begin(db);
		if (srunlikely(tx == NULL))
			goto error;
		while (sr_iterhas(&i)) {
			v = sr_iterof(&i);
			assert(svlsn(v) == lsn);
			rc = sp_set(tx, v);
			if (srunlikely(rc == -1))
				goto rlb;
			sr_gcmark(&log->gc, 1);
			sr_iternext(&i);
		}
		if (srunlikely(sl_itererror(&i)))
			goto rlb;
		rc = sp_commit(tx, lsn, log);
		if (srunlikely(rc != 0))
			goto error;

		rc = sl_itercontinue(&i);
		if (srunlikely(rc == -1))
			goto error;
		if (rc == 0)
			break;
	}
	sr_iterclose(&i);
	return 0;
rlb:
	sp_rollback(tx);
error:
	sr_iterclose(&i);
	return -1;
}

static inline int
se_recoverlogpool(se *e)
{
	srlist *i;
	sr_listforeach(&e->lp.list, i) {
		sl *log = srcast(i, sl, link);
		int rc = se_recoverlog(e, log);
		if (srunlikely(rc == -1))
			return -1;
		sr_gccomplete(&log->gc);
	}
	sr_seq(&e->seq, SR_LSNNEXT);
	return 0;
}

int se_recover(se *e)
{
	/* iterate storage and each storage file in 
	 * reverse order.
	 * track latest page versions.
	*/ 
	int rc = se_recoverstorage(e);
	if (srunlikely(rc == -1))
		return -1;
	/* recreate database schemas */
	rc = se_recoverscheme(e);
	if (srunlikely(rc == -1))
		return -1;
	/* recreate databases */
	rc = se_recoverbuild(e);
	if (srunlikely(rc == -1))
		return -1;
	/* reply log files */
	rc = se_recoverlogpool(e);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}
