
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

typedef struct ssdbiter ssdbiter;

struct ssdbiter {
	int validate;
	srfile *db;
	srmap map;
	srorder order;
	sspage current;
	void *v;
} srpacked;

static void
ss_dbiter_init(sriter *i)
{
	assert(sizeof(ssdbiter) <= sizeof(i->priv));
	ssdbiter *ii = (ssdbiter*)i->priv;
	memset(ii, 0, sizeof(*ii));
}

static inline int
ss_dbiter_gt(ssdbiter *i)
{
	i->v = i->map.p;
	sspageheader *h = (sspageheader*)i->v;
	if (i->validate) {
		uint32_t crc = sr_crcs(h, sizeof(sspageheader), 0);
		if (srunlikely(crc != h->crc))
			return -1;
	}
	ss_pageinit(&i->current, i->db, h);
	return 0;
}

static inline int
ss_dbiter_lt(ssdbiter *i)
{
	i->v = (char*)i->map.p + i->map.size - sizeof(sspagefooter);
	sspagefooter *f = (sspagefooter*)i->v;
	if (f->marker != SS_PAGEMARKER)
		return -1;
	sspageheader *h = (sspageheader*)((char*)i->v - f->size);
	i->v = h;
	if (i->validate) {
		uint32_t crc = sr_crcs(h, sizeof(sspageheader), 0);
		if (srunlikely(crc != h->crc))
			return -1;
	}
	ss_pageinit(&i->current, i->db, h);
	return 0;
}

static int
ss_dbiter_open(sriter *i, va_list args)
{
	ssdbiter *ii = (ssdbiter*)i->priv;
	ii->db       = va_arg(args, srfile*);
	ii->order    = va_arg(args, srorder);
	ii->validate = va_arg(args, int);
	ii->v        = NULL;
	if (srunlikely(ii->db->size == 0))
		return 0;
	if (ii->db->size < (sizeof(sspageheader) + sizeof(sspagefooter)))
		return -1;
	int rc = sr_map(&ii->map, ii->db->fd, ii->db->size, 1);
	if (srunlikely(rc == -1))
		return -1;
	switch (ii->order) {
	case SR_GT: rc = ss_dbiter_gt(ii);
		break;
	case SR_LT: rc = ss_dbiter_lt(ii);
		break;
	default:    rc = -1;
		break;
	}
	if (srunlikely(rc == -1))
		sr_mapunmap(&ii->map);
	return 0;
}

static void
ss_dbiter_close(sriter *i srunused)
{
	ssdbiter *ii = (ssdbiter*)i->priv;
	sr_mapunmap(&ii->map);
}

static int
ss_dbiter_has(sriter *i)
{
	ssdbiter *ii = (ssdbiter*)i->priv;
	return ii->v != NULL;
}

static void*
ss_dbiter_of(sriter *i)
{
	ssdbiter *ii = (ssdbiter*)i->priv;
	if (srunlikely(ii->v == NULL))
		return NULL;
	return &ii->current;
}

static void
ss_dbiter_next(sriter *i)
{
	ssdbiter *ii = (ssdbiter*)i->priv;
	if (srunlikely(ii->v == NULL))
		return;
	switch (ii->order) {
	case SR_GT:
		ii->v = (char*)ii->v + sizeof(sspageheader) +
		        ii->current.h->size;
		if (srunlikely((char*)ii->v >= ((char*)ii->map.p + ii->map.size))) {
			ii->v = NULL;
			return;
		}
		break;
	case SR_LT: {
		ii->v = (char*)ii->v - sizeof(sspagefooter);
		if (srunlikely((char*)ii->v < (char*)ii->map.p)) {
			ii->v = NULL;
			return;
		}
		sspagefooter *f = (sspagefooter*)ii->v;
		if (srunlikely(f->marker != SS_PAGEMARKER)) {
			ii->v = NULL;
			return;
		}
		ii->v = (char*)ii->v - f->size;
		if (srunlikely((char*)ii->v < (char*)ii->map.p)) {
			ii->v = NULL;
			return;
		}
		break;
	}
	default: assert(0);
	}
	if (ii->validate) {
		sspageheader *h = (sspageheader*)ii->v;
		uint32_t crc = sr_crcs(h, sizeof(sspageheader), 0);
		if (srunlikely(crc != h->crc)) {
			ii->v = NULL;
			return;
		}
	}
	ss_pageinit(&ii->current, ii->db, ii->v);
}

sriterif ss_dbiter =
{
	.init    = ss_dbiter_init,
	.open    = ss_dbiter_open,
	.close   = ss_dbiter_close,
	.has     = ss_dbiter_has,
	.of      = ss_dbiter_of,
	.next    = ss_dbiter_next
};

uint64_t ss_dbiter_offset(sriter *i)
{
	ssdbiter *ii = (ssdbiter*)i->priv;
	if (srunlikely(ii->v == NULL))
		return 0;
	return (char*)ii->v - (char*)ii->map.p;
}
