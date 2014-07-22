
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

typedef struct srschemefmt srschemefmt;

struct srschemefmt {
	uint32_t dsn;
	uint64_t lsnc;
	uint64_t lsnd;
	uint8_t  namelen;
	uint8_t  cmplen;
} srpacked;

int sr_schemeinit(srscheme *s, sra *a, char *name, srcmpindex *i, char *cmp)
{
	s->dsn   = 0;
	s->lsnc  = 0;
	s->lsnd  = 0;
	s->name  = sr_strdup(a, name);
	if (srunlikely(s->name == NULL))
		return -1;
	s->cmp   = sr_cmpindex_match(i, cmp);
	s->undef = 0;
	return 0;
}

int sr_schemeinit_from(srscheme *s, sra *a, srcmpindex *i,
                       void *buf, int bufsize)
{
	if (bufsize < (int)sizeof(srschemefmt))
		return -1;
	srschemefmt *h = (srschemefmt*)buf;
	s->undef = 0;
	s->dsn   = h->dsn;
	s->lsnc  = h->lsnc;
	s->lsnd  = h->lsnd;
	s->name  = sr_malloc(a, h->namelen);
	if (srunlikely(s->name == NULL))
		return -1;
	memcpy(s->name, (char*)buf + sizeof(srschemefmt), h->namelen);
	char *cmpname = sr_malloc(a, h->cmplen);
	if (srunlikely(s->name == NULL))
		return -1;
	memcpy(cmpname, (char*)buf + sizeof(srschemefmt) + h->namelen,
	       h->cmplen);
	s->cmp = sr_cmpindex_match(i, cmpname);
	sr_free(a, cmpname);
	if (srunlikely(s->cmp == NULL)) {
		sr_free(a, s->name);
		return -1;
	}
	return 0;
}

int sr_schemeinit_undef(srscheme *s, sra *a, srcmpindex *i)
{
	int rc = sr_schemeinit(s, a, "undef", i, "string");
	s->undef = 1;
	return rc;
}

int sr_schemecopy(srscheme *s, sra *a, srscheme *src)
{
	s->undef = src->undef;
	s->dsn   = src->dsn;
	s->lsnc  = src->lsnc;
	s->lsnd  = src->lsnd;
	s->cmp   = src->cmp;
	s->name  = sr_strdup(a, src->name);
	if (srunlikely(s->name == NULL))
		return -1;
	return 0;
}

int sr_schemefree(srscheme *s, sra *a)
{
	if (s->name)
		sr_free(a, s->name);
	return 0;
}

int sr_schemeserialize(srscheme *s, sra *a, srbuf *buf)
{
	int namelen = strlen(s->name) + 1;
	int cmplen  = strlen(s->cmp->name) + 1;
	int size = sizeof(srschemefmt) + namelen + cmplen;
	sr_bufinit(buf);
	int rc = sr_bufensure(buf, a, size);
	if (srunlikely(rc == -1))
		return -1;
	srschemefmt *h = (srschemefmt*)buf->s;
	h->dsn     = s->dsn;
	h->lsnc    = s->lsnc;
	h->lsnd    = s->lsnd;
	h->namelen = namelen;
	h->cmplen  = cmplen;
	memcpy(buf->s + sizeof(srschemefmt), s->name, namelen);
	memcpy(buf->s + sizeof(srschemefmt) + namelen,
	       s->cmp->name, cmplen);
	sr_bufadvance(buf, size);
	return 0;
}

char *sr_schemeprepare(srschemeparser *p, va_list args)
{
	va_copy(p->args, args);
	p->path = va_arg(p->args, char*);
	snprintf(p->q, sizeof(p->q), "%s", p->path);
	char *name;
	name = strtok_r(p->q, ".", &p->qp);
	if (srunlikely(name == NULL))
		return NULL;
	return name;
}

int sr_schemeset(srschemeparser *p, srcmpindex *i, srscheme *s)
{
	char *token;
	token = strtok_r(NULL, ".", &p->qp);
	if (srunlikely(token == NULL))
		return 0;
	if (strcmp(token, "cmp") == 0) {
		char *name = va_arg(p->args, char*);
		srcomparator *c = sr_cmpindex_match(i, name);
		if (srunlikely(c == NULL))
			return -1;
		s->cmp = c;
	} else 
	if (strcmp(token, "type") == 0) {
	} else  {
		return -1;
	}
	return 0;
}
