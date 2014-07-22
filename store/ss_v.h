#ifndef SS_V_H_
#define SS_V_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssv ssv;

struct ssv {
	uint32_t crc;
	uint64_t lsn;
	uint8_t  flags;
	uint32_t valuesize;
	uint64_t valueoffset;
	uint16_t keysize;
	char     key[];
} srpacked;

extern svif ss_vif;

static inline int
ss_isstore(sv *v) {
	return v->i == &ss_vif;
}

#endif
