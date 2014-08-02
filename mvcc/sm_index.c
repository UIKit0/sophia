
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>

int sm_indexinit(smindex *i, src *c)
{
	i->keymax = 0;
	i->lsnmax = 0;
	i->lsnmin = (uint64_t)-1;
	int rc = sr_iinit(&i->index, c->a, 256, &c->sdb->cmp);
	return rc;
}

int sm_indexfree(smindex *i)
{
	sr_ifree(&i->index);
	return 0;
}

int sm_indexfreei(smindex *i)
{
	sr_ifreei(&i->index);
	return 0;
}

int sm_indextruncate(smindex *i)
{
	return sr_itruncate(&i->index);
}

int sm_indexreplace(smindex *i, sra *a, uint64_t lsvn, smv *v)
{
	sr_ii pos;
	sr_iopen(&pos, &i->index);
	smv *head = NULL;
	int rc = sr_iprepare(&pos, v->key, v->keysize);
	switch (rc) {
	case  1: /* exists */
		head = sr_ival(&pos);
		assert(head != NULL);
		head = sm_vupdate(a, head, v, lsvn);
		sr_ivalset(&pos, head);
		break;
	case  0:
		sr_ivalset(&pos, v);
		break;
	case -1: return -1;
	}
	if (srunlikely(v->keysize > i->keymax))
		i->keymax = v->keysize;
	if (srunlikely(v->id.lsn > i->lsnmax))
		i->lsnmax = v->id.lsn;
	if (srunlikely(v->id.lsn < i->lsnmin))
		i->lsnmin = v->id.lsn;
	return 0;
}

int sm_indexstat(smindex *i, smindexstat *s)
{
	s->n = i->index.n;
	return 0;
}
