
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>

typedef struct svseaveiter svseaveiter;

struct svseaveiter {
	sriter *merge;
	uint32_t limit; 
	uint32_t unique;
	uint64_t lsvn;
	int boundreached;
	int boundon;
	void *bound;
	int boundsize;
	int next;
	sv *v;
} srpacked;

static void
sv_seaveiter_init(sriter *i)
{
	assert(sizeof(svseaveiter) <= sizeof(i->priv));
	svseaveiter *im = (svseaveiter*)i->priv;
	memset(im, 0, sizeof(*im));
}

static void sv_seaveiter_next(sriter*);

static int
sv_seaveiter_open(sriter *i, va_list args)
{
	svseaveiter *im = (svseaveiter*)i->priv;
	im->merge     = va_arg(args, sriter*);
	im->limit     = va_arg(args, uint32_t);
	im->lsvn      = va_arg(args, uint64_t);
	im->boundon   = va_arg(args, int);
	im->bound     = va_arg(args, void*);
	im->boundsize = va_arg(args, int);
	sv_seaveiter_next(i);
	return 0;
}

static void
sv_seaveiter_close(sriter *i srunused)
{ }

static int
sv_seaveiter_has(sriter *i)
{
	svseaveiter *im = (svseaveiter*)i->priv;
	return im->v != NULL;
}

static void*
sv_seaveiter_of(sriter *i)
{
	svseaveiter *im = (svseaveiter*)i->priv;
	if (srunlikely(im->v == NULL))
		return NULL;
	return im->v;
}

static void
sv_seaveiter_next(sriter *i)
{
	svseaveiter *im = (svseaveiter*)i->priv;
	if (im->next)
		sr_iternext(im->merge);
	im->next = 0;
	im->v = NULL;

	while (sr_iterhas(im->merge))
	{
		sv *v = sr_iterof(im->merge);
		uint8_t dup = (svflags(v) & SVDUP) > 0;

		if (im->unique >= im->limit) {
			if (! dup)
				break;
		}

		if (srunlikely(dup)) {
			if (svlsn(v) < im->lsvn) {
				sr_iternext(im->merge);
				continue;
			}
		} else {
			if (im->boundon) {
				int rc = sr_compare(&i->c->sdb->cmp, svkey(v), svkeysize(v),
				                    im->bound, im->boundsize);
				if (srunlikely(rc >= 0))
					im->boundreached = 1;

				if (srunlikely(rc == 1))
					break;
			}

			uint8_t del = (svflags(v) & SVDELETE) > 0;
			if (srunlikely(del && (svlsn(v) < im->lsvn))) {
				sr_iternext(im->merge);
				continue;
			}

			im->unique++;
		}

		im->v = v;
		im->next = 1;
		break;
	}
}

sriterif sv_seaveiter =
{
	.init    = sv_seaveiter_init,
	.open    = sv_seaveiter_open,
	.close   = sv_seaveiter_close,
	.has     = sv_seaveiter_has,
	.of      = sv_seaveiter_of,
	.next    = sv_seaveiter_next
};

int sv_seaveiter_resume(sriter *i)
{
	svseaveiter *im = (svseaveiter*)i->priv;
	im->v      = sr_iterof(im->merge);
	im->unique = 1;
	im->next   = 1;
	return im->boundreached;
}
