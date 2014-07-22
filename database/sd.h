#ifndef SD_H_
#define SD_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#define SD_FDROP 1

typedef struct sd sd;

struct sd {
	uint32_t id;
	srflags flags;
	sdindex primary;
	srbuf rbuf;
	ss *store;
	srscheme *scheme;
	src *c;
};

int sd_new(sd*, srscheme*, ss*, src*);
int sd_prepare(sd*);
int sd_free(sd*);
int sd_recover(sd*, sstrack*);
int sd_drop(sd*, ssc*);
int sd_snapshot(sd*, ssc*);

#endif
