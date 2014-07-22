#ifndef SD_NODEINDEX_H_
#define SD_NODEINDEX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdnodeindex sdnodeindex;

struct sdnodeindex {
	sdnode **i;
	int n;
};

static inline sdnode*
sd_nodeindexv(sdnodeindex *i, int pos)
{
	assert(pos < i->n);
	return i->i[pos];
}

int sd_nodeindex_init(sdnodeindex*);
int sd_nodeindex_free(sdnodeindex*, sra*);
int sd_nodeindex_freeindex(sdnodeindex*, sra*);
int sd_nodeindex_clone(sdnodeindex*, sra*, sdnodeindex*);
int sd_nodeindex_prepare(sdnodeindex*, sra*, sdnode*);
int sd_nodeindex_append(sdnodeindex*, sra*, sdnode*);
int sd_nodeindex_update(sdnodeindex*, sra*, sdnode*, int, sdnode**);
int sd_nodeindex_delete(sdnodeindex*, sra*, sdnode*);

#endif
