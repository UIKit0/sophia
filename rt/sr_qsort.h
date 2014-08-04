#ifndef SR_QSORT_H_
#define SR_QSORT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

void
sr_qsort(void*, size_t, size_t,
         int (*cmp)(const void*, const void*, void*),
         void*);

#endif
