
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>

typedef struct svmergeiter svmergeiter;

#define SV_MERGEITER_A 1
#define SV_MERGEITER_B 2

struct svmergeiter {
	int mask;
	sriter *a, *b;
	sriter *v;
} srpacked;

static void
sv_mergeiter_init(sriter *i)
{
	assert(sizeof(svmergeiter) <= sizeof(i->priv));
	svmergeiter *im = (svmergeiter*)i->priv;
	memset(im, 0, sizeof(*im));
}

static void sv_mergeiter_next(sriter*);

static int
sv_mergeiter_open(sriter *i, va_list args)
{
	svmergeiter *im = (svmergeiter*)i->priv;
	im->a = va_arg(args, sriter*);
	im->b = va_arg(args, sriter*);
	sv_mergeiter_next(i);
	return 0;
}

static void
sv_mergeiter_close(sriter *i srunused)
{ }

static int
sv_mergeiter_has(sriter *i)
{
	svmergeiter *im = (svmergeiter*)i->priv;
	return im->v != NULL;
}

static void*
sv_mergeiter_of(sriter *i)
{
	svmergeiter *im = (svmergeiter*)i->priv;
	if (srunlikely(im->v == NULL))
		return NULL;
	return sr_iterof(im->v);
}

#if 0
static inline int
sv_mergeiter_recover(svmergeiter *i)
{
	/* Skip older duplicates from source a, which should
	 * have newer versions (in-memory index).
	 *
	 * This condition is only possible when
	 * recovering from log (a), which were already merged
	 * before (b).
	 */
	sv *a = sr_iterof(i->a);
	sv *b = sr_iterof(i->b);
	int skip = 0;
	while (a && svlsn(a) <= svlsn(b))
	{
		skip++;
		sr_iternext(i->a);
		a = sr_iterof(i->a);
	}
	return skip;
}
#endif

static void
sv_mergeiter_next(sriter *i)
{
	svmergeiter *im = (svmergeiter*)i->priv;
	if (im->mask & SV_MERGEITER_A)
		sr_iternext(im->a);
	if (im->mask & SV_MERGEITER_B)
		sr_iternext(im->b);
	im->mask = 0;
	sv *a = sr_iterof(im->a);
	sv *b = sr_iterof(im->b);
	if (a && b) {
		assert(a->v != NULL);
		assert(b->v != NULL);
		int rc = svcompare(a, b, &i->c->sdb->cmp);
		switch (rc) {
		case  0:
			/*
			if (srunlikely(sv_mergeiter_recover(im)))
				goto retry;
			*/
			im->v = im->a;
			im->mask = SV_MERGEITER_A;
			svsetdup(b);
			break;
		case -1:
			im->v = im->a;
			im->mask = SV_MERGEITER_A;
			break;
		case  1:
			im->v = im->b;
			im->mask = SV_MERGEITER_B;
			break;
		}
		return;
	}
	if (a) {
		im->v = im->a;
		im->mask = SV_MERGEITER_A;
		return;
	}
	if (b) {
		im->v = im->b;
		im->mask = SV_MERGEITER_B;
		return;
	}
	im->v = NULL;
}

sriterif sv_mergeiter =
{
	.init    = sv_mergeiter_init,
	.open    = sv_mergeiter_open,
	.close   = sv_mergeiter_close,
	.has     = sv_mergeiter_has,
	.of      = sv_mergeiter_of,
	.next    = sv_mergeiter_next
};
