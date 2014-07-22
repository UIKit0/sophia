#ifndef SE_BACKUP_H_
#define SE_BACKUP_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sebackup sebackup;

struct sebackup {
	seobj o;
	int ready;
	srbuf list;
	char **v;
	se *e;
};

seobj *se_backupnew(se*);

#endif
