#ifndef SS_COPY_H_
#define SS_COPY_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sscopy sscopy;

struct sscopy {
	ss *s;
	ssdblist *dbl;
	srbuf *rwbuf;
	srbuf *ref;
	src *c;
	ssindex *index;
};

int ss_copyinit(sscopy*, ss*, src*, ssc*, ssdblist*, ssindex*);
int ss_copy(sscopy*);
int ss_copywrite(sscopy*, sswritef, void*);
int ss_copycommit(sscopy*, uint32_t, uint64_t);

#endif
