#ifndef SE_CURSOR_H_
#define SE_CURSOR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct secursor secursor;

struct secursor {
	seobj o;
	int ready;
	srorder order;
	smtx t;
	sv v;
	sedb *db;
};

seobj *se_cursornew(sedb*, int, void*, int);

#endif
