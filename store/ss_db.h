#ifndef SS_DB_H_
#define SS_DB_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#define SS_FGC 1

typedef struct ssdb ssdb;
typedef struct ssdbref ssdbref;

struct ssdb {
	uint32_t id;
	srspinlock lock;
	int ref;
	srfile file;
	srflags flags;
	srgc gc;
	srlist link;
};

struct ssdbref {
	uint32_t dfsn;
	uint32_t mark;
	uint32_t sweep;
} srpacked;

ssdb *ss_dballoc(src*, uint32_t);
ssdb *ss_dbnew(src*);
ssdb *ss_dbopen(src*, uint32_t);
int ss_dbclose(ssdb*, src*);

int ss_dbwrite(ssdb*, sspagebuild*, uint64_t*);
int ss_dbwritebuf(ssdb*, srbuf*, int, uint64_t*);
int ss_dbgc(ssdb*, src*);

static inline void
ss_dbref(ssdb *db)
{
	sr_spinlock(&db->lock);
	db->ref++;
	sr_spinunlock(&db->lock);
}

static inline int
ss_dbunref(ssdb *db)
{
	sr_spinlock(&db->lock);
	int ref = --db->ref;
	assert(db->ref >= 0);
	sr_spinunlock(&db->lock);
	return ref;
}

static inline int
ss_dbrefof(ssdb *db)
{
	sr_spinlock(&db->lock);
	int ref = db->ref;
	sr_spinunlock(&db->lock);
	return ref;
}

#endif
