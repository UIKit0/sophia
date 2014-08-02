#ifndef LIBSM_H_
#define LIBSM_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <sm_v.h>

struct smtx;
extern int sm_cmp(struct smtx*, uint32_t);

#define sri_instance t
#define sri_type struct smtx*
#define sri_cmp(i, t, id, unused) sm_cmp(t, *(uint32_t*)id)
#include <sr_i.h>

#define sri_instance i
#define sri_type smv*
#define sri_free(a, obj) sm_vfree(a, obj)
#define sri_cmp(i, obj, key, size) \
	sr_compare(i->cmp, obj->key, obj->keysize, key, size)
#include <sr_i.h>

#include <sm_index.h>
#include <sm_indexiter.h>
#include <sm.h>
#include <sm_iter.h>

#endif
