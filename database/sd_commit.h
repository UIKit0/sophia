#ifndef SD_COMMIT_H_
#define SD_COMMIT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdtx sdtx;

struct sdtx {
	uint64_t lsvn;
	svlog *log;
	sv *last;
	sd *db;
	sl *l;
	sdstat reserve[32];
	srbuf buf;
};

int sd_begin(sd*, sdtx*, uint64_t, svlog*, sl*);
int sd_end(sdtx*);
int sd_commit(sdtx*);
int sd_rollback(sdtx*);
int sd_write(sdtx*);

#endif
