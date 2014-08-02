#ifndef SE_DB_H_
#define SE_DB_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sedbctl sedbctl;
typedef struct sedb sedb;
typedef struct sedbconf sedbconf;

struct sedbctl {
	seobj o;
	sedb *parent;
};

struct sedbconf {
	seobj o;
	sedb *parent;
};

struct sedb {
	seobj o;
	srspinlock lock;
	int ref;
	srscheme scheme;
	sstrack track;
	sm mvcc;
	sd db;
	se *e;
	src c;
	sedbconf objconf;
	sedbctl  objctl;
};

seobj *se_dbnew(se*, uint32_t);
seobj *se_dbmatch(se*, uint32_t);
int    se_dbdrop(sedb*);
void   se_dbref(sedb*);
int    se_dbunref(sedb*);

#endif
