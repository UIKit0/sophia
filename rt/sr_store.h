#ifndef SR_STORE_H_
#define SR_STORE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srstoretype srstoretype;
typedef struct srstoreid srstoreid;

struct srstoretype {
	char *ext;
	uint32_t mask;
	int count;
	// max
};

struct srstoreid {
	uint32_t mask;
	uint64_t id;
};

int sr_storeread(srbuf*, sra*, srstoretype*, char*);

#endif
