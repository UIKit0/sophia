
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsl.h>
#include <libsm.h>

static uint8_t
sm_vifflags(sv *v) {
	return ((smv*)v->v)->flags;
}

static uint64_t
sm_viflsn(sv *v) {
	return ((smv*)v->v)->id.lsn;
}

static void
sm_viflsnset(sv *v, uint64_t lsn) {
	((smv*)v->v)->id.lsn = lsn;
}

static uint32_t
sm_vifdsn(sv *v) {
	assert(0);
	(void)v;
	return 0;
}

static char*
sm_vifkey(sv *v) {
	return ((smv*)v->v)->key;
}

static uint16_t
sm_vifkeysize(sv *v) {
	return ((smv*)v->v)->keysize;
}

static char*
sm_vifvalue(sv *v)
{
	register smv *nv = v->v;
	return sm_vv(nv);
}

static int
sm_vifvaluecopy(sv *v, char *dest)
{
	register smv *nv = v->v;
	void *value = sm_vv(nv);
	memcpy(dest, value, nv->valuesize);
	return 0;
}

static uint32_t
sm_vifvaluesize(sv *v) {
	return ((smv*)v->v)->valuesize;
}

static uint64_t
sm_vifoffset(sv *v) {
	(void)v;
	assert(0);
	return 0;
}

static char*
sm_vifraw(sv *v) {
	(void)v;
	assert(0);
	return 0;
}

static uint32_t
sm_vifrawsize(sv *v) {
	(void)v;
	assert(0);
	return 0;
}

static void
sm_vifref(sv *v) {
	register smv *nv = v->v;
	sm_vref(nv);
}

static void
sm_vifunref(sv *v, sra *a) {
	register smv *nv = v->v;
	sm_vunref(a, nv);
}

static void
sm_vifsetdup(sv *v) {
	smv *nv = v->v;
	nv->flags |= SVDUP;
}

svif sm_vif =
{
	.flags       = sm_vifflags,
	.lsn         = sm_viflsn,
	.lsnset      = sm_viflsnset,
	.dsn         = sm_vifdsn,
	.key         = sm_vifkey,
	.keysize     = sm_vifkeysize,
	.value       = sm_vifvalue,
	.valuecopy   = sm_vifvaluecopy,
	.valuesize   = sm_vifvaluesize,
	.valueoffset = sm_vifoffset,
	.raw         = sm_vifraw,
	.rawsize     = sm_vifrawsize,
	.ref         = sm_vifref,
	.unref       = sm_vifunref,
	.setdup      = sm_vifsetdup
};

void sm_vsweep(smv *v)
{
	if (v->log)
		sr_gcsweep(&((sl*)v->log)->gc, 1);
}
