#ifndef SR_SCHEME_H_
#define SR_SCHEME_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srscheme srscheme;
typedef struct srschemeparser srschemeparser;

struct srscheme {
	uint32_t dsn;
	uint64_t lsnc;
	uint64_t lsnd;
	char *name;
	srcomparator *cmp;
	int undef;
};

struct srschemeparser {
	char q[200];
	char *qp;
	char *path;
	va_list args;
};

int sr_schemeinit(srscheme*, sra*, char*, srcmpindex*, char*);
int sr_schemeinit_from(srscheme*, sra*, srcmpindex*, void*, int);
int sr_schemeinit_undef(srscheme*, sra*, srcmpindex*);
int sr_schemecopy(srscheme*, sra*, srscheme*);
int sr_schemefree(srscheme*, sra*);
int sr_schemeserialize(srscheme*, sra*, srbuf*);
char *sr_schemeprepare(srschemeparser*, va_list);
int sr_schemeset(srschemeparser*, srcmpindex*, srscheme*);

#endif
