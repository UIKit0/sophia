#ifndef SM_DB_H_
#define SM_DB_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct smdb smdb;
typedef struct smdbindex smdbindex;

struct smdb {
	srscheme *s;
	int ref;
	void *ptr;
	smdb *next;
	srlist link;
};

struct smdbindex {
	srspinlock lock;
	srlist i;
	int n;
};

static inline void
sm_dbinit(smdb *db, srscheme *s, void *ptr)
{
	db->ref  = 0;
	db->s    = s;
	db->ptr  = ptr;
	db->next = NULL;
	sr_listinit(&db->link);
}

static inline smdb*
sm_dbland(smdb *db, uint64_t lsvn)
{
	while (db && db->s->lsnc > lsvn)
		db = db->next;
	if (db && db->s->lsnd == 0)
		return db;
	return NULL;
}

static inline void
sm_dbindex_init(smdbindex *i)
{
	sr_spinlockinit(&i->lock);
	sr_listinit(&i->i);
	i->n = 0;
}

static inline void
sm_dbindex_free(smdbindex *i)
{
	sr_spinlockfree(&i->lock);
}

static inline void
sm_dbindex_add(smdbindex *i, smdb *db)
{
	sr_spinlock(&i->lock);
	srlist *p;
	sr_listforeach(&i->i, p) {
		smdb *ref = srcast(p, smdb, link);
		if (strcmp(ref->s->name, db->s->name) == 0)
		{
			sr_listunlink(&ref->link);
			db->next = ref;
			goto match;
		}
	}
match:
	sr_listappend(&i->i, &db->link);
	i->n++;
	sr_spinunlock(&i->lock);
}

static inline void
sm_dbindex_remove(smdbindex *i, smdb *db)
{
	sr_spinlock(&i->lock);
	srlist *p;
	sr_listforeach(&i->i, p) {
		smdb *ref = srcast(p, smdb, link);
		if (strcmp(ref->s->name, db->s->name) == 0)
		{
			smdb *prev = NULL;
			smdb *c = ref;
			while (c) {
				if (c == db) {
					if (prev) {
						prev->next = c->next;
					} else {
						sr_listunlink(&c->link);
						c = c->next;
						if (c) {
							sr_listappend(&i->i, &c->link);
						}
					}
					break;
				}
				prev = c;
				c = c->next;
			}
			i->n--;
			break;
		}
	}
	sr_spinunlock(&i->lock);
}

static inline int
sm_dbindex_garbage(smdbindex *i, smdb *db)
{
	sr_spinlock(&i->lock);
	int v = db->s->lsnd > 0 && db->ref == 0;
	sr_spinunlock(&i->lock);
	return v;
}

static inline smdb*
sm_dbindex_search(smdbindex *i, uint32_t dsn)
{
	srlist *p;
	sr_listforeach(&i->i, p) {
		smdb *ref = srcast(p, smdb, link);
		while (ref && ref->s->dsn != dsn)
			ref = ref->next;
		if (ref)
			return ref;
	}
	return NULL;
}

static inline smdb*
sm_dbindex_find(smdbindex *i, char *name, uint64_t lsvn)
{
	srlist *p;
	sr_listforeach(&i->i, p) {
		smdb *ref = srcast(p, smdb, link);
		if (strcmp(ref->s->name, name) == 0)
			return sm_dbland(ref, lsvn);
	}
	return NULL;
}

static inline int
sm_dbindex_drop(smdbindex *i, uint32_t dsn, uint64_t lsnd)
{
	sr_spinlock(&i->lock);
	smdb *db = sm_dbindex_search(i, dsn);
	if (srunlikely(db == NULL)) {
		sr_spinunlock(&i->lock);
		return 0;
	}
	assert(db->s->lsnd == 0);
	db->s->lsnd = lsnd;
	sr_spinunlock(&i->lock);
	return 1;
}

static inline int
sm_dbindex_land(smdbindex *i, char *name, uint64_t lsvn, uint32_t *dsn)
{
	sr_spinlock(&i->lock);
	smdb *db = sm_dbindex_find(i, name, lsvn);
	if (srunlikely(db == NULL)) {
		sr_spinunlock(&i->lock);
		return 0;
	}
	if (dsn)
		*dsn = db->s->dsn;
	sr_spinunlock(&i->lock);
	return 1;
}

static inline smdb*
sm_dbindex_use(smdbindex *i, char *name, uint64_t lsvn)
{
	sr_spinlock(&i->lock);
	smdb *db = sm_dbindex_find(i, name, lsvn);
	if (db == NULL)  {
		sr_spinunlock(&i->lock);
		return NULL;;
	}
	db->ref++;
	sr_spinunlock(&i->lock);
	return db;
}

static inline void
sm_dbindex_unuse(smdbindex *i, smdb *db)
{
	sr_spinlock(&i->lock);
	db->ref--;
	assert(db->ref >= 0);
	sr_spinunlock(&i->lock);
}

#endif
