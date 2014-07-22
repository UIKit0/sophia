#ifndef SM_V_H_
#define SM_V_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct smv smv;

struct smv {
	smv *next, *prev;
	void *log;
	uint8_t  flags;
	uint16_t ref;
	union {
		uint64_t lsn;
		struct {
			uint32_t id;
			uint32_t lo;
		} tx;
	} id;
	uint32_t valuesize;
	uint16_t keysize;
	char     key[];
} srpacked;

extern svif sm_vif;

static inline smv*
sm_valloc(sra *a, sv *v)
{
	int keysize = svkeysize(v);
	int valuesize = svvaluesize(v);
	int size = sizeof(smv) + keysize + valuesize;
	smv *mv = sr_malloc(a, size);
	if (srunlikely(mv == NULL))
		return NULL;
	memset(&mv->id, 0, sizeof(mv->id));
	mv->id.lsn    = svlsn(v);
	mv->ref       = 0;
	mv->flags     = svflags(v);
	mv->keysize   = keysize; 
	mv->valuesize = valuesize;
	mv->log       = NULL;
	mv->next      = NULL;
	mv->prev      = NULL;
	memcpy(mv->key, svkey(v), mv->keysize);
	int rc = svvaluecopy(v, mv->key + keysize);
	if (srunlikely(rc == -1)) {
		sr_free(a, mv);
		mv = NULL;
	}
	return mv;
}

static inline void*
sm_vv(smv *v) {
	return (char*)(v->key + v->keysize);
}

static inline void
sm_vref(smv *v)
{
	v->ref++;
}

void sm_vsweep(smv*);

static inline void
sm_vunref(sra *a, smv *v)
{
	if (srunlikely(v->ref == 0)) {
		sm_vsweep(v);
		sr_free(a, v);
	} else
		v->ref--;
}

static inline void
sm_vfree(sra *a, smv *v)
{
	while (v) {
		register smv *n = v->next;
		sm_vunref(a, v);
		v = n;
	}
}

static inline smv*
sm_vland(smv *v, uint64_t lsvn) {
	while (v && v->id.lsn > lsvn)
		v = v->next;
	return v;
}

static inline smv*
sm_vupdate(sra *a, smv *head, smv *n, uint64_t lsvn)
{
	assert(head->id.lsn < n->id.lsn);
	/*assert(n->id.lsn >= lsvn);*/
	if (srlikely(n->id.lsn == lsvn)) {
		sm_vfree(a, head);
		return n;
	}
	n->next = head;
	register smv *c = head;
	while (c && c->id.lsn > lsvn)
		c = c->next;
	if (c == NULL)
		return n;
	/* <= lsvn */
	register smv *p = c->next;
	c->next = NULL;
	sm_vfree(a, p);
	return n;
}

#if 0
static inline smv*
sm_vupdate(sra *a, smv *head, smv *n, uint64_t lsvn)
{
	assert(head->id.lsn < n->id.lsn);
	assert(n->id.lsn >= lsvn);
	if (srlikely(n->id.lsn == lsvn)) {
		sm_vfree(a, head);
		return n;
	}
	n->next = head;
	register smv *c = head;
	while (c && c->id.lsn > lsvn)
		c = c->next;
	if (c == NULL)
		return n;
	/* <= lsvn */
	register smv *p = c->next;
	c->next = NULL;
	sm_vfree(a, p);
	return n;
}
#endif

static inline smv*
sm_vmatch(smv *head, uint32_t id) {
	register smv *c = head;
	while (c) {
		if (c->id.tx.id == id)
			break;
		c = c->next;
	}
	return c;
}

static inline void
sm_vreplace(smv *v, smv *n) {
	if (v->prev)
		v->prev->next = n;
	if (v->next)
		v->next->prev = n;
	n->next = v->next;
	n->prev = v->prev;
}

static inline void
sm_vlink(smv *head, smv *v) {
	register smv *c = head;
	while (c->next)
		c = c->next;
	c->next = v;
	v->prev = c;
}

static inline void
sm_vunlink(smv *v) {
	if (v->prev)
		v->prev->next = v->next;
	if (v->next)
		v->next->prev = v->prev;
}

static inline void
sm_vabortwaiters(smv *v) {
	register smv *c = v->next;
	while (c) {
		c->flags |= SVABORT;
		c = c->next;
	}
}

#endif
