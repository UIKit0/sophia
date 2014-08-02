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

#define SR_SCHEMEDSN 1
#define SR_SCHEMECMP 2

struct srscheme {
	uint32_t dsn;
	srcomparator cmp;
	int bm;
	int ready;
	int recover;
};

struct srschemeparser {
	char q[200];
	char *qp;
	char *path;
	va_list args;
};

int sr_schemeinit(srscheme*, sra*);
int sr_schemecopy(srscheme*, sra*, srscheme*);
int sr_schemefree(srscheme*, sra*);
char *sr_schemeprepare(srschemeparser*, va_list);
int sr_schemeset(srschemeparser*, srscheme*);

#endif
