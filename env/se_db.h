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

struct sedbctl {
	seobj o;
	sedb *parent;
};

struct sedb {
	seobj o;
	srspinlock lock;
	int ref;
	srscheme scheme;
	sstrack track;
	smdb dbv;
	sm mvcc;
	sd db;
	se *e;
	src c;
	sedbctl objctl;
};

seobj *se_dbnew(se*, srscheme*);
seobj *se_dbprepare(se*, uint32_t);
seobj *se_dbdeploy(sedb*, srscheme*);
seobj *se_dbmatch(se*, char*);
seobj *se_dbmatchid(se*, uint32_t);
int    se_dbdrop(sedb*);
void   se_dbref(sedb*);
int    se_dbunref(sedb*);

#endif
