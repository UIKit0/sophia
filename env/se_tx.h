#ifndef SE_TX_H_
#define SE_TX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct setx setx;

typedef int (*setxpostcommitf)(setx*, void*);

struct setx {
	seobj o;
	smtx t;
	setxpostcommitf pc;
	void *pcarg;
	sedb *db;
};

seobj *se_txnew(sedb*, setxpostcommitf, void*);
int se_txssset(sedb*, uint8_t, va_list);
int se_txssget(sedb*, va_list);

#endif
