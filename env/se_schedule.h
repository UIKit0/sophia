#ifndef SE_SCHEDULE_H_
#define SE_SCHEDULE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#define SE_ROTATELOG  1
#define SE_ROTATEDB   2
#define SE_GC         4
#define SE_MERGE      8
#define SE_MERGEDB    16
#define SE_DROP       32
#define SE_SNAPSHOT   64
#define SE_ALL        SE_ROTATELOG | SE_ROTATEDB | SE_GC | \
                      SE_MERGE | SE_DROP | \
                      SE_SNAPSHOT
#define SE_FORCE      128

typedef struct {
	int plan;
	se *e;
	sedb *db;
	setask *t;
} seplan;

static inline void
se_planinit(seplan *p, se *e, int plan)
{
	p->plan = plan;
	p->e    = e;
	p->db   = NULL;
	p->t    = NULL;
}

int se_schedule(seplan*);
int se_scheduledone(se*, setask*);

#endif
