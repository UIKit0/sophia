
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

int sr_schemeinit(srscheme *s, sra *a)
{
	(void)a;
	s->dsn     = 0;
	s->cmp.cmp = sr_cmpstring;
	s->cmp.cmparg = NULL;
	s->bm      = 0;
	s->ready   = 0;
	s->recover = 0;
	return 0;
}

int sr_schemecopy(srscheme *s, sra *a, srscheme *src)
{
	(void)a;
	s->dsn     = src->dsn;
	s->cmp     = src->cmp;
	s->bm      = src->bm;
	s->ready   = src->ready;
	s->recover = src->recover;
	return 0;
}

int sr_schemefree(srscheme *s, sra *a)
{
	(void)s;
	(void)a;
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

int sr_schemeset(srschemeparser *p, srscheme *s)
{
	char *token;
	token = strtok_r(NULL, ".", &p->qp);
	if (srunlikely(token == NULL))
		return 0;
	if (strcmp(token, "cmp") == 0) {
		s->cmp.cmp = va_arg(p->args, srcmpf);
		s->cmp.cmparg = va_arg(p->args, void*);
		s->bm |= SR_SCHEMECMP;
	} else 
	if (strcmp(token, "id") == 0) {
		s->dsn = va_arg(p->args, uint32_t);
		s->bm |= SR_SCHEMEDSN;
	} else  {
		return -1;
	}
	return 0;
}
