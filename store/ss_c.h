#ifndef SS_C_H_
#define SS_C_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssc ssc;

struct ssc {
	sspagebuild build;
	srbuf a; /* rbuf, rwbuf */
	srbuf b; /* gc, ref */
};

static inline void
ss_cinit(ssc *sc, src *c)
{
	ss_pagebuild_init(&sc->build, c);
	sr_bufinit(&sc->a);
	sr_bufinit(&sc->b);
}

static inline void
ss_cfree(ssc *sc, src *c)
{
	ss_pagebuild_free(&sc->build);
	sr_buffree(&sc->a, c->a);
	sr_buffree(&sc->b, c->a);
}

static inline void
ss_creset(ssc *sc)
{
	ss_pagebuild_reset(&sc->build);
	sr_bufreset(&sc->a);
	sr_bufreset(&sc->b);
}

#endif
