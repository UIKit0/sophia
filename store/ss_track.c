
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libss.h>

void ss_trackgc(sstrack *t)
{
	int zeroes = 0;
	int gc = 0;
	int i = 0;
	int j = 0;
	for (; i < t->size; i++) {
		if (t->i[i] == NULL) {
			zeroes++;
			continue;
		}
		if (srunlikely(ss_refisdelete(t->i[i]))) {
			sr_free(t->c->a, t->i[i]);
			t->i[i] = NULL;
			gc++;
			continue;
		}
		t->i[j] = t->i[i];
		j++;
	}
	t->size -= zeroes;
	t->size -= gc;
}

void ss_trackcompact(sstrack *t)
{
	int zeroes = 0;
	int i = 0;
	int j = 0;
	for (; i < t->size; i++) {
		if (t->i[i] == NULL) {
			zeroes++;
			continue;
		}
		t->i[j] = t->i[i];
		j++;
	}
	t->size -= zeroes;
}

static inline int
ss_trackcmp(const void *a, const void *b, void *c)
{
	register ssref *ap = *(ssref**)a;
	register ssref *bp = *(ssref**)b;
	register sstrack *t = c;
	register int rc =
		sr_compare(&t->c->sdb->cmp,
		           ss_refmin(ap), ap->sizemin,
		           ss_refmin(bp), bp->sizemin);
	assert(rc != 0);
	return rc;
}

void ss_tracksort(sstrack *t)
{
	sr_qsort(t->i, t->size, sizeof(ssref*), ss_trackcmp, t);
}
