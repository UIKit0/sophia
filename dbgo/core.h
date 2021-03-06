#ifndef SP_CORE_H_
#define SP_CORE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#define SP_VERSION_MAJOR 1
#define SP_VERSION_MINOR 1

enum sptxn {
	SPTSS, SPTMS
};

typedef enum sptxn sptxn;

struct sp {
	spmagic m;
	spenv *env;
	spa a;
	sprep rep;
	sptxn txn;          /* transaction mode: single or multi-stmt */
	spi *i, i0, i1;
	spi itxn;
	int iskip;          /* skip second index during read */
	uint64_t psn;       /* page sequence number */
	spcat s;
	spbatch lb;         /* log batch related */
	volatile int stop;
	sptask merger;
	sprefset refs;      /* pre allocated key buffer (page merge) */
	spfile lockdb;      /* per-process database lock */
	int lockc;          /* incremental cursor lock */
	spspinlock lockr;   /* repository lock */
	spspinlock locks;   /* space lock */
	spspinlock locki;   /* index lock */
	spe e, em;          /* separate error contexts: fe and merger */
};

int sp_rotate(sp*, spe*);

static inline int sp_active(sp *s) {
	return !s->stop;
}

static inline spi*
sp_ipair(sp *s) {
	return (s->i == &s->i0 ? &s->i1 : &s->i0);
}

static inline spi*
sp_iswap(sp *s) {
	spi *old = s->i;
	s->i = sp_ipair(s);
	return old;
}

static inline void
sp_iskipset(sp *s, int v) {
	sp_lock(&s->locki);
	s->iskip = v;
	sp_unlock(&s->locki);
}

static inline void
sp_glock(sp *s) {
	if (s->lockc > 0)
		return;
	sp_lock(&s->lockr);
	sp_replockall(&s->rep);
	sp_lock(&s->locki);
	sp_lock(&s->locks);
	s->lockc++;
}

static inline void
sp_gunlock(sp *s) {
	s->lockc--;
	if (s->lockc > 0)
		return;
	sp_unlock(&s->locks);
	sp_unlock(&s->locki);
	sp_repunlockall(&s->rep);
	sp_unlock(&s->lockr);
}

static inline int
sp_evalidate(sp *s) {
	return sp_echeck(&s->e) + sp_echeck(&s->em);
}

static inline int
sp_e(sp *s, int type, ...) {
	va_list args;
	va_start(args, type);
	sp_vef(&s->e, type, args);
	va_end(args);
	return -1;
}

static inline int
sp_em(sp *s, int type, ...) {
	va_list args;
	va_start(args, type);
	sp_vef(&s->em, type, args);
	va_end(args);
	return -1;
}


#endif
