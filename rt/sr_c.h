#ifndef SR_C_H_
#define SR_C_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct src src;

struct src {
	srconf *c;
	srscheme *sdb;
	srseq *seq;
	sra *a;
};

static inline void
sr_cinit(src *c, srconf *conf, srscheme *sdb,
         srseq *seq, sra *a)
{
	c->c   = conf;
	c->sdb = sdb;
	c->seq = seq;
	c->a   = a;
}

#endif
