#ifndef SR_CONF_H_
#define SR_CONF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srconf srconf;

struct srconf {
	srallocf a;
	void *aarg;
	char *logdir;
	int logdir_read;
	int logdir_write;
	int logdir_create;
	char *dir;
	int dir_read;
	int dir_write;
	int dir_create;

	int node_merge_wm;
	int node_merge_force_round;
	int node_merge_force_min;

	int node_size;
	int page_size;
	int log_gc;
	int log_rotate_wm;
	int db_gc;
	float db_gc_factor;
	int db_rotate_wm;

	int snapshot;
	int snapshot_wm;

	int threads;
	int scheduler;
	int scheduler_tick;
};

int sr_confinit(srconf*, sra*);
int sr_conffree(srconf*, sra*);
int sr_confset(srconf*, sra*, char*, va_list);
int sr_confvalidate(srconf*);

#endif
