#ifndef SE_CMD_H_
#define SE_CMD_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef enum {
	SE_CMDNONE,
	SE_CMDMERGE,
	SE_CMDGC,
	SE_CMDROTATELOG,
	SE_CMDROTATEDB,
	SE_CMDDROP,
	SE_CMDSNAPSHOT
} secmdid;

typedef int (*secmdf)(void*, void*);

typedef struct {
	secmdid id;
	secmdf f;
	char *name;
} secmd;

secmd *se_cmdof(secmdid);

#endif
