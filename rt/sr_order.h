#ifndef SR_ORDER_H_
#define SR_ORDER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef enum {
	SR_LT,
	SR_LTE,
	SR_GT,
	SR_GTE,
	SR_EQ,
	SR_UPDATE
} srorder;

#endif
