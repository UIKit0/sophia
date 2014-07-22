
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

int sd_nodeindex_init(sdnodeindex *i)
{
	i->n = 0;
	i->i = NULL;
	return 0;
}

int sd_nodeindex_freeindex(sdnodeindex *i, sra *a)
{
	if (srunlikely(i->i == NULL))
		return 0;
	sr_free(a, i->i);
	return 0;
}

int sd_nodeindex_free(sdnodeindex *i, sra *a)
{
	if (srunlikely(i->i == NULL))
		return 0;
	int current = 0;
	int rcret = 0;
	int rc = 0;
	while (current < i->n) {
		sdnode *n = i->i[current];
		rc = sd_nodefree(n);
		if (srunlikely(rc == -1))
			rcret = -1;
		sr_free(a, n);
		current++;
	}
	sd_nodeindex_freeindex(i, a);
	return rcret;
}

int sd_nodeindex_clone(sdnodeindex *i, sra *a, sdnodeindex *c)
{
	c->n = i->n;
	c->i = sr_malloc(a, c->n * sizeof(sdnode*));
	if (srunlikely(c->i == NULL))
		return -1;
	int j = 0;
	while (j < i->n) {
		c->i[j] = i->i[j];
		j++;
	}
	return 0;
}

int sd_nodeindex_prepare(sdnodeindex *i, sra *a, sdnode *n)
{
	sdnode **index = sr_malloc(a, sizeof(sdnode*));
	if (srunlikely(index == NULL))
		return -1;
	i->i = index;
	i->i[0] = n;
	i->n = 1;
	return 0;
}

int sd_nodeindex_append(sdnodeindex *i, sra *a, sdnode *n)
{
	int indexn = i->n + 1;
	sdnode **index = sr_malloc(a, indexn * sizeof(sdnode*));
	if (srunlikely(index == NULL))
		return -1;
	memcpy(index, i->i, i->n * sizeof(sdnode*));
	i->i = index;
	i->i[i->n] = n;
	i->n++;
	return 0;
}

int sd_nodeindex_update(sdnodeindex *i, sra *a, sdnode *origin, int nc, sdnode **nv)
{
	if (nc == 0)
		return 0;

	int indexn = i->n + nc;
	sdnode **indexold;
	sdnode **index = sr_malloc(a, indexn * sizeof(sdnode*));
	if (srunlikely(index == NULL))
		return -1;
	int j = 0;
	int k = 0;
	while (j < i->n) {
		sdnode *ptr = i->i[j];
		index[k] = ptr;
		k++;
		j++;
		if (srunlikely(ptr == origin)) {
			int g = 0;
			while (g < nc) {
				index[k] = nv[g];
				g++;
				k++;
			}
		}
	}
	assert(k == indexn);

	indexold = i->i;
	i->i = index;
	i->n = indexn;

	/*assert(nc == 1);*/

#if 0
	assert( *(int*)ss_indexpage_min(ss_indexmin(&origin->index)) <
			*(int*)ss_indexpage_min(ss_indexmin(&nv[0]->index)) );

	assert( *(int*)ss_indexpage_max(ss_indexmax(&origin->index)) <
			*(int*)ss_indexpage_max(ss_indexmax(&nv[0]->index)) );

	assert( *(int*)ss_indexpage_max(ss_indexmax(&origin->index)) <
			*(int*)ss_indexpage_min(ss_indexmin(&nv[0]->index)) );

	j = 1;
	while (j < indexn) {

		if (index[j - 1]->ip.i->index.n > 0) {
			smv *max = sr_imax(&index[j - 1]->ip.i->index);
			assert(*(int*)max->key < *(int*)ss_indexpage_min(ss_indexmin(&index[j]->index)) );
		}
		j++;
	}

	j = 1;
	while (j < indexn) {
		assert( *(int*)ss_indexpage_min(ss_indexmin(&index[j - 1]->index)) <
				*(int*)ss_indexpage_min(ss_indexmin(&index[j]->index)) );

		assert( *(int*)ss_indexpage_max(ss_indexmax(&index[j - 1]->index)) <
				*(int*)ss_indexpage_max(ss_indexmax(&index[j]->index)) );

		assert( *(int*)ss_indexpage_max(ss_indexmax(&index[j - 1]->index)) <
				*(int*)ss_indexpage_min(ss_indexmin(&index[j]->index)) );
		j++;
	}
#endif

	sr_free(a, indexold);
	return 0;
}

int sd_nodeindex_delete(sdnodeindex *i, sra *a, sdnode *origin)
{
	assert(i->n > 1);

	int indexn = i->n - 1;
	sdnode **indexold;
	sdnode **index = sr_malloc(a, indexn * sizeof(sdnode*));
	if (srunlikely(index == NULL))
		return -1;
	int j = 0;
	int k = 0;
	while (j < i->n) {
		sdnode *ptr = i->i[j];
		if (srlikely(ptr != origin)) {
			index[k] = ptr;
			k++;
		}
		j++;
	}
	assert(k == indexn);

	indexold = i->i;
	i->i = index;
	i->n = indexn;

	sr_free(a, indexold);
	return 0;
}
