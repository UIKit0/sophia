
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libss.h>

int ss_indexadd(ssindex *i, sra *a, ssref *p)
{
	int rc = sr_bufensure(&i->i, a, sizeof(ssref**));
	if (srunlikely(rc == -1))
		return -1;
	*(ssref**)(i->i.p) = p;
	sr_bufadvance(&i->i, sizeof(ssref**));
	ss_refref(p);
	i->n++;
	return 0;
}

int ss_indexrebase(ssindex *i, uint32_t dfsn, uint64_t offset)
{
	/* update new pages */
	int p = 0;
	while (p < i->n) {
		ssref *page = ss_indexpage(i, p);
		if (page->dfsn == UINT32_MAX) {
			page->dfsn = dfsn;
			page->offset += offset;
		}
		ss_indexaccount(i, page);
		p++;
	}
	return 0;
}

int ss_indexcopy(ssindex *dest, sra *a, ssindex *src, int from, int n)
{
	assert((from + n) <= src->n); 
	int sz = sizeof(ssref**) * n;
	int rc = sr_bufensure(&dest->i, a, sz);
	if (srunlikely(rc == -1))
		return -1;
	dest->n       = n;
	dest->lsnmin  = (uint64_t)-1;
	dest->lsnmax  = 0;
	dest->dfsnmin = (uint32_t)-1;
	dest->dfsnmax = 0;
	int end = from + n;
	while (from < end) 
	{
		ssref *p = ss_indexpage(src, from);
		ss_indexaccount(dest, p);
		*(ssref**)(dest->i.p) = p;
		ss_refref(p);
		sr_bufadvance(&dest->i, sizeof(ssref**));
		from++;
	}
	return 0;
}
