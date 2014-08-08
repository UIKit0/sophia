#ifndef SE_OBJ_H_
#define SE_OBJ_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef enum {
	SEOUNDEF    = 0L,
	SEO         = 0x06154834L,
	SEOCTL      = 0x643189AFL,
	SEOCONF     = 0x43845160L,
	SEODB       = 0x00fec0feL,
	SEODBCTL    = 0x132790A7L,
	SEODBCONF   = 0x34342108L,
	SEOTX       = 0x58391255L,
	SEOCURSOR   = 0x2128761FL,
	SEOBACKUP   = 0x4C725999L
} seobjid;

static inline seobjid
se_objof(void *ptr) {
	return *(seobjid*)ptr;
}

typedef struct seobjif seobjif;
typedef struct seobj seobj;
typedef struct seobjindex seobjindex;

struct seobjif {
	int     (*open)(seobj*, va_list);
	int     (*destroy)(seobj*);
	void   *(*database)(seobj*, va_list);
	void   *(*ctl)(seobj*, va_list);
	int     (*drop)(seobj*, va_list);
	int     (*set)(seobj*, va_list);
	int     (*get)(seobj*, va_list);
	int     (*del)(seobj*, va_list);
	void   *(*begin)(seobj*);
	int     (*commit)(seobj*, va_list);
	int     (*rollback)(seobj*);
	void   *(*cursor)(seobj*, int, void*, int);
	int     (*fetch)(seobj*);
	void   *(*key)(seobj*);
	size_t  (*keysize)(seobj*);
	void   *(*value)(seobj*);
	size_t  (*valuesize)(seobj*);
	void   *(*backup)(seobj*);
};

struct seobj {
	seobjid  oid;
	srlist   olink;
	seobjif *oif;
};

struct seobjindex {
	srspinlock lock;
	srlist dblist;
	int dbn;
	srlist txlist;
	int txn;
	srlist cursorlist;
	int cursorn;
	srlist backuplist;
	int backupn;
};

static inline void
se_objindex_init(seobjindex *i)
{
	sr_spinlockinit(&i->lock);
	sr_listinit(&i->dblist);
	sr_listinit(&i->txlist);
	sr_listinit(&i->cursorlist);
	sr_listinit(&i->backuplist);
	i->dbn       = 0;
	i->txn       = 0;
	i->cursorn   = 0;
	i->backupn   = 0;
}

static inline void
se_objindex_free(seobjindex *i)
{
	sr_spinlockfree(&i->lock);
	sr_listinit(&i->dblist);
	sr_listinit(&i->txlist);
	sr_listinit(&i->cursorlist);
	sr_listinit(&i->backuplist);
	i->dbn       = 0;
	i->txn       = 0;
	i->cursorn   = 0;
	i->backupn   = 0;
}

static inline void
se_objindex_lock(seobjindex *i) {
	sr_spinlock(&i->lock);
}

static inline void
se_objindex_unlock(seobjindex *i) {
	sr_spinunlock(&i->lock);
}

static inline void
se_objindex_register(seobjindex *i, seobj *o)
{
	se_objindex_lock(i);
	switch (o->oid) {
	case SEODB:
		sr_listappend(&i->dblist, &o->olink);
		i->dbn++;
		break;
	case SEOTX:
		sr_listappend(&i->txlist, &o->olink);
		i->txn++;
		break;
	case SEOCURSOR:
		sr_listappend(&i->cursorlist, &o->olink);
		i->cursorn++;
		break;
	case SEOBACKUP:
		sr_listappend(&i->backuplist, &o->olink);
		i->backupn++;
		break;
	default: assert(0);
	}
	se_objindex_unlock(i);
}

static inline void
se_objindex_unregister(seobjindex *i, seobj *o)
{
	se_objindex_lock(i);
	switch (o->oid) {
	case SEODB:       i->dbn--;
		break;
	case SEOTX:       i->txn--;
		break;
	case SEOCURSOR:   i->cursorn--;
		break;
	case SEOBACKUP:   i->backupn--;
		break;
	default: assert(0);
	}
	sr_listunlink(&o->olink);
	se_objindex_unlock(i);
}

static inline int
se_objindex_count(seobjindex *i, seobjid id)
{
	se_objindex_lock(i);
	int v = 0;
	switch (id) {
	case SEODB:       v = i->dbn;
		break;
	case SEOTX:       v = i->txn;
		break;
	case SEOCURSOR:   v = i->cursorn;
		break;
	case SEOBACKUP:   v = i->backupn;
		break;
	default: assert(0);
	}
	se_objindex_unlock(i);
	return v;
}

static inline void
se_objinit(seobj *o, seobjid oid, seobjif *oif)
{
	o->oid = oid;
	o->oif = oif;
	sr_listinit(&o->olink);
}

#endif
