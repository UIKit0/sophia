#ifndef SS_DBLIST_H_
#define SS_DBLIST_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssdblist ssdblist;

struct ssdblist {
	srspinlock lock;
	int ref;
	srbuf list;
	uint32_t dfsnmin;
	uint32_t dfsnmax;
};

static inline ssdblist*
ss_dblist_new(sra *a)
{
	ssdblist *l = sr_malloc(a, sizeof(ssdblist));
	if (srunlikely(l == NULL))
		return NULL;
	sr_spinlockinit(&l->lock);
	l->ref = 0;
	l->dfsnmin = (uint32_t)-1;
	l->dfsnmax = 0;
	sr_bufinit(&l->list);
	return l;
}

static inline void
ss_dblist_free(ssdblist *l, sra *a)
{
	sr_spinlockfree(&l->lock);
	sr_buffree(&l->list, a);
	sr_free(a, l);
}

static inline void
ss_dblist_ref(ssdblist *l)
{
	sr_spinlock(&l->lock);
	l->ref++;
	sr_spinunlock(&l->lock);
}

static inline void
ss_dblist_unref(ssdblist *l, sra *a)
{
	sr_spinlock(&l->lock);
	if (srlikely(l->ref <= 1)) {
		sr_spinunlock(&l->lock);
		ss_dblist_free(l, a);
		return;
	}
	l->ref--;
	sr_spinunlock(&l->lock);
}

static inline int
ss_dblist_add(ssdblist *l, sra *a, ssdb *db)
{
	if (db->id < l->dfsnmin)
		l->dfsnmin = db->id;
	if (db->id > l->dfsnmax)
		l->dfsnmax = db->id;
	return sr_bufadd(&l->list, a, &db, sizeof(ssdb*));
}

static inline ssdb*
ss_dblist_in(ssdblist *l, uint32_t dfsn)
{
	sriter i;
	sr_iterinit(&i, &sr_bufiterref, NULL);
	sr_iteropen(&i, &l->list, sizeof(ssdb**));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		ssdb *db = sr_iterof(&i);
		if (db->id == dfsn)
			return db;
	}
	return NULL;
}

#endif
