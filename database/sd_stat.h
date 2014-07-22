#ifndef SD_STAT_H_
#define SD_STAT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdstat sdstat;

struct sdstat {
	smindexstat i;
	void *node;
} srpacked;

#endif
