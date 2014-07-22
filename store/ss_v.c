
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

static uint8_t
ss_vifflags(sv *v) {
	return ((ssv*)v->v)->flags;
}

static uint64_t
ss_viflsn(sv *v) {
	return ((ssv*)v->v)->lsn;
}

static void
ss_viflsnset(sv *v, uint64_t lsn) {
	(void)v;
	(void)lsn;
	assert(0);
}

static uint32_t
ss_vifdsn(sv *v) {
	assert(0);
	(void)v;
	return 0;
}

static char*
ss_vifkey(sv *v) {
	return ((ssv*)v->v)->key;
}

static uint16_t
ss_vifkeysize(sv *v) {
	return ((ssv*)v->v)->keysize;
}

static char*
ss_vifvalue(sv *v)
{
	(void)v;
	assert(0);
	return NULL;
}

static int
ss_vifvaluecopy(sv *v, char *dest)
{
	ssv *dv = v->v;
	char *value = ss_pagevalue(v->arg, dv);
	memcpy(dest, value, dv->valuesize);
	return 0;
	/*
	srfile *f = v->arg;
	return sr_filepread(f, dv->valueoffset, dest, dv->valuesize);
	*/
}

static uint32_t
ss_vifvaluesize(sv *v) {
	return ((ssv*)v->v)->valuesize;
}

static uint64_t
ss_vifoffset(sv *v) {
	return ((ssv*)v->v)->valueoffset;
}

static char*
ss_vifraw(sv *v) {
	return v->v;
}

static uint32_t
ss_vifrawsize(sv *v) {
	return sizeof(ssv) + ((ssv*)v->v)->keysize;
}

static void
ss_vifref(sv *v) {
	(void)v;
}

static void
ss_vifunref(sv *v, sra *a) {
	(void)v;
	(void)a;
}

static void
ss_vifsetdup(sv *v) {
	ssv *dv = v->v;
	dv->flags |= SVDUP;
}

svif ss_vif =
{
	.flags       = ss_vifflags,
	.lsn         = ss_viflsn,
	.lsnset      = ss_viflsnset,
	.dsn         = ss_vifdsn,
	.key         = ss_vifkey,
	.keysize     = ss_vifkeysize,
	.value       = ss_vifvalue,
	.valuecopy   = ss_vifvaluecopy,
	.valuesize   = ss_vifvaluesize,
	.valueoffset = ss_vifoffset,
	.raw         = ss_vifraw,
	.rawsize     = ss_vifrawsize,
	.ref         = ss_vifref,
	.unref       = ss_vifunref,
	.setdup      = ss_vifsetdup
};
