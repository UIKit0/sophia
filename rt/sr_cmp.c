
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

int srhot
sr_cmpu32(char *a, size_t asz, char *b, size_t bsz,
          void *arg srunused)
{
	(void)asz;
	(void)bsz;
	register uint32_t av = *(uint32_t*)a;
	register uint32_t bv = *(uint32_t*)b;
	if (av == bv)
		return 0;
	return (av > bv) ? 1 : -1;
}

int srhot
sr_cmpstring(char *a, size_t asz, char *b, size_t bsz,
             void *arg srunused)
{
	register size_t sz = (asz < bsz ? asz : bsz);
	register int rc = memcmp(a, b, sz);
	return (rc == 0 ? rc : (rc > 0 ? 1 : -1));
}

int sr_cmpindex_add(srcmpindex *i, sra *a, srcmptype type, char *name,
                    srcmpf cmp, void *cmparg)
{
	srcomparator *c = sr_malloc(a, sizeof(srcomparator));
	if (srunlikely(c == NULL))
		return -1;
	c->name = sr_strdup(a, name);
	if (srunlikely(c->name == NULL)) {
		sr_free(a, c);
		return -1;
	}
	c->type = type;
	c->cmp = cmp;
	c->cmparg = cmparg;
	sr_listinit(&c->link);
	sr_listappend(&i->i, &c->link);
	return 0;
}

int sr_cmpindex_init(srcmpindex *i, sra *a)
{
	sr_listinit(&i->i);
	int rc = sr_cmpindex_add(i, a, SR_CMPU32, "u32", sr_cmpu32, NULL);
	if (srunlikely(rc == -1))
		return -1;
	rc = sr_cmpindex_add(i, a, SR_CMPSTRING, "string", sr_cmpstring, NULL);
	if (srunlikely(rc == -1))
		goto error;
	return 0;
error:
	sr_cmpindex_free(i, a);
	return -1;
}

int sr_cmpindex_free(srcmpindex *i, sra *a)
{
	srlist *p, *n;
	sr_listforeach_safe(&i->i, p, n) {
		srcomparator *c = srcast(p, srcomparator, link);
		sr_free(a, c->name);
		sr_free(a, c);
	}
	sr_listinit(&i->i);
	return 0;
}

srcomparator*
sr_cmpindex_match(srcmpindex *i, char *name)
{
	srlist *p;
	sr_listforeach(&i->i, p) {
		srcomparator *c = srcast(p, srcomparator, link);
		if (strcmp(c->name, name) == 0)
			return c;
	}
	return NULL;
}

srcomparator*
sr_cmpindex_matchtype(srcmpindex *i, srcmptype type)
{
	srlist *p;
	sr_listforeach(&i->i, p) {
		srcomparator *c = srcast(p, srcomparator, link);
		if (c->type == type)
			return c;
	}
	return NULL;
}
