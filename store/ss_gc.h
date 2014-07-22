#ifndef SS_GC_H_
#define SS_GC_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int ss_gcschedule(ss*, sra*, float, ssdblist**);
int ss_gc(ss*, src*);

#endif
