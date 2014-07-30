#ifndef SE_H_
#define SE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sectl sectl;
typedef struct seconf seconf;
typedef struct secmp secmp;
typedef struct se se;

struct sectl {
	seobj o;
	se *e;
};

struct seconf {
	seobj o;
	se *e;
};

struct secmp {
	seobj o;
	se *e;
};

typedef enum {
	SE_OFFLINE,
	SE_DEPLOY,
	SE_ONLINE,
	SE_RECOVER,
	SE_SHUTDOWN
} semode;

struct se {
	seobj o;
	volatile semode mode;
	sra a;
	sre e;
	srseq seq;
	srcmpindex ci;
	slpool lp;
	ss store;
	setaskmgr tm;
	seprocessmgr pm;
	seprocess stub;
	srflags schedflags;
	int schedsnap;
	smdbindex dbvi;
	src c;
	srconf conf;
	seconf objconf;
	secmp  objcmp;
	seobj *objscheme;
	sectl  objctl;
	seobjindex oi;
};

static inline int
se_active(se *e) {
	return e->mode != SE_OFFLINE &&
	       e->mode != SE_SHUTDOWN;
}

seobj *se_new(void);

int se_list(se*, srbuf*);
int se_listunref(se*, srbuf*);
int se_snapshot(se*, ssc*);

#endif