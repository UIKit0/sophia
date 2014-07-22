#ifndef SE_SCHEME_H_
#define SE_SCHEME_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct seschemetx seschemetx;
typedef struct sescheme sescheme;

struct seschemetx {
	seobj o;
	sescheme *s;
	setx *t;
};

struct sescheme {
	seobj o;
	se *e;
	sedb *scheme;
};

seobj *se_schemenew(se*);
seobj *se_schemeprepare(se*);

#endif
