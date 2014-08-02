
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsl.h>
#include <libss.h>
#include <libsm.h>
#include <libsd.h>

int sd_splitinit(sdsplit *s, src *c, ssindex *i)
{
	s->i     = i;
	s->c     = c;
	s->count = 0;
	s->n     = NULL;
	ss_indexinit(&s->iorigin, 0);
	return sd_pairinit(&s->iporigin, c);
}

int sd_splitfree(sdsplit *s)
{
	int i = 0;
	while (i < s->count)
	{
		sd_nodefree(s->n[i]);
		sr_free(s->c->a, s->n[i]);
		i++;
	}
	s->count = 0;
	if (s->n) {
		sr_free(s->c->a, s->n);
		s->n = NULL;
	}
	ss_indexfree(&s->iorigin, s->c->a);
	sd_pairfree(&s->iporigin, 1);
	return 0;
}

int sd_splitgc(sdsplit *s)
{
	sr_free(s->c->a, s->n);
	s->n = NULL;
	s->count = 0;
	return 0;
}

int sd_split(sdsplit *s)
{
	int n = s->i->n / s->c->c->node_size;
	assert(n >= 2);
	n = n - 1;

	/* allocate and init nodes */
	s->n = sr_malloc(s->c->a, sizeof(sdnode*) * n);
	if (srunlikely(s->n == NULL))
		return -1;
	int rc;
	int i = 0;
	while (i < n) {
		s->n[i] = sr_malloc(s->c->a, sizeof(sdnode));
		if (srunlikely(s->n[i] == NULL))
			return -1;
		uint32_t nsn = sr_seq(s->c->seq, SR_NSNNEXT);
		rc = sd_nodeinit(s->n[i], s->c, nsn);
		if (srunlikely(rc == -1)) {
			sr_free(s->c->a, s->n[i]);
			return -1;
		}
		s->count++;
		i++;
	}

	/* split page index (origin) */
	int copy = s->c->c->node_size;
	int pos  = 0;
	rc = ss_indexcopy(&s->iorigin, s->c->a, s->i, pos, copy);
	if (srunlikely(rc == -1))
		return -1;

	/* split page index */
	pos = copy;
	i   = 0;
	while (i < n) {
		copy = s->c->c->node_size;
		if (i == (n - 1))
			copy = s->i->n - pos;
		rc = ss_indexcopy(&s->n[i]->index, s->c->a, s->i, pos, copy);
		if (srunlikely(rc == -1))
			return -1;
		pos += copy;
		i++;
	}
	assert(pos == s->i->n);

	return s->count;
}

#if 0
int sd_split(sdsplit *s)
{
	int n = s->i->n / s->c->c->node_size;
	int m = s->i->n % s->c->c->node_size;
	if (m > 0)
		n++;
	assert(n >= 2);
	n = n - 1;

	/* allocate and init nodes */
	s->n = sr_malloc(s->c->a, sizeof(sdnode*) * n);
	if (srunlikely(s->n == NULL))
		return -1;
	int rc;
	int i = 0;
	while (i < n) {
		s->n[i] = sr_malloc(s->c->a, sizeof(sdnode));
		if (srunlikely(s->n[i] == NULL))
			return -1;
		uint32_t nsn = sr_seq(s->c->seq, SR_NSNNEXT);
		rc = sd_nodeinit(s->n[i], s->c, nsn);
		if (srunlikely(rc == -1)) {
			sr_free(s->c->a, s->n[i]);
			return -1;
		}
		s->count++;
		i++;
	}

	/* split page index (origin) */
	int copy = s->c->c->node_size;
	int pos  = 0;
	rc = ss_indexcopy(&s->iorigin, s->c->a, s->i, pos, copy);
	if (srunlikely(rc == -1))
		return -1;

	/* split page index */
	pos = copy;
	i   = 0;
	while (i < n) {
		copy = s->c->c->node_size;
		if (i == (n - 1))
			copy = s->i->n - pos;
		rc = ss_indexcopy(&s->n[i]->index, s->c->a, s->i, pos, copy);
		if (srunlikely(rc == -1))
			return -1;
		pos += copy;
		i++;
	}
	assert(pos == s->i->n);

	return s->count;
}
#endif

int sd_splitrange(sdsplit *s, smindex *i)
{
	assert(s->count >= 1);
	if (srunlikely(i->index.n == 0))
		return 0;

	srcomparator *cmp = &s->c->sdb->cmp;
	sriter j;
	sr_iterinit(&j, &sm_indexiterraw, s->c);
	sr_iteropen(&j, i);

	sdpair *pair = &s->iporigin;
	ssref *min = ss_indexmin(&s->n[0]->index);
	char *bound = ss_refmin(min);
	int pos = -1;
	int boundsize = min->sizemin;
	int rc;

	for (;;)
	{
		while (sr_iterhas(&j))
		{
			sv *v = sr_iterof(&j);
			rc = sr_compare(cmp, bound, boundsize, svkey(v), svkeysize(v));
			if (rc <= 0)
				break;
			rc = sm_indexreplace(pair->i, s->c->a, UINT64_MAX, (smv*)v->v);
			if (srunlikely(rc == -1))
				return -1;
			sr_iternext(&j);
		}
		pos++;
		if (srunlikely(pos == (s->count - 1)))
			break;
		pair      = &s->n[pos]->ip;
		min       = ss_indexmin(&s->n[pos + 1]->index);
		bound     = ss_refmin(min);
		boundsize = min->sizemin;
	}

	pair = &s->n[s->count - 1]->ip;
	while (sr_iterhas(&j))
	{
		sv *v = sr_iterof(&j);
		rc = sm_indexreplace(pair->i, s->c->a, UINT64_MAX, (smv*)v->v);
		if (srunlikely(rc == -1))
			return -1;
		sr_iternext(&j);
	}
	return 0;
}
