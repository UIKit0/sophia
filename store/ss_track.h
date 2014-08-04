#ifndef SS_TRACK_H_
#define SS_TRACK_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sstrack sstrack;

struct sstrack {
	ssref **i;
	int size;
	int n;
	uint16_t keymax;
	uint32_t psnmax;
	src *c;
};

static inline void
ss_trackinit(sstrack *t, src *c)
{
	t->c = c;
	t->keymax = 0;
	t->psnmax = 0;
	t->n = 0;
	t->size = 0;
	t->i = NULL;
}

static inline int
ss_trackprepare(sstrack *t, int size)
{
	t->size = size;
	int sz = sizeof(ssref*) * size;
	t->i = sr_malloc(t->c->a, sz);
	if (srunlikely(t->i == NULL))
		return -1;
	memset(t->i, 0, sz);
	return 0;
}

static inline void
ss_trackfreeindex(sstrack *t)
{
	if (srunlikely(t->i == NULL))
		return;
	sr_free(t->c->a, t->i);
	t->i = NULL;
}

static inline void
ss_trackfree(sstrack *t)
{
	if (srunlikely(t->i == NULL))
		return;
	int i = 0;
	while (i < t->size) {
		if (t->i[i] == NULL) {
			i++;
			continue;
		}
		sr_free(t->c->a, t->i[i]);
		i++;
	}
	ss_trackfreeindex(t);
}

static inline void
ss_trackinsert(sstrack *t, ssref *p)
{
	uint32_t pos = p->psn % t->size;
	for (;;) {
		if (srunlikely(t->i[pos] != NULL)) {
			pos = (pos + 1) % t->size;
			continue;
		}
		if (p->psn > t->psnmax)
			t->psnmax = p->psn;
		t->i[pos] = p;
		break;
	}
	t->n++;
}

static inline int
ss_trackresize(sstrack *t) {
	sstrack nt;
	ss_trackinit(&nt, t->c);
	int rc = ss_trackprepare(&nt, t->size * 2);
	if (srunlikely(rc == -1))
		return -1;
	int i = 0;
	while (i < t->size) {
		if (t->i[i])
			ss_trackinsert(&nt, t->i[i]);
		i++;
	}
	ss_trackfreeindex(t);
	*t = nt;
	return 0;
}

static inline int
ss_trackset(sstrack *t, ssref *p)
{
	if (srunlikely(t->n > (t->size / 2)))
		if (srunlikely(ss_trackresize(t) == -1))
			return -1;
	if (p->sizemin > t->keymax)
		t->keymax = p->sizemin;
	if (p->sizemax > t->keymax)
		t->keymax = p->sizemax;
	ss_trackinsert(t, p);
	return 0;
}

static inline int
ss_trackhas(sstrack *t, uint32_t id)
{
	uint32_t pos = id % t->size;
	for (;;) {
		if (srunlikely(t->i[pos] == NULL))
			return 0;
		if (t->i[pos]->psn == id)
			return 1;
		pos = (pos + 1) % t->size;
		continue;
	}
	return 0;
}

void ss_tracksort(sstrack*, srcomparator*);
void ss_trackgc(sstrack*);

#endif
