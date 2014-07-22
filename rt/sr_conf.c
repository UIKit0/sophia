
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

int sr_confinit(srconf *c, sra *a)
{
	c->a              = sr_allocstd;
	c->aarg           = NULL;
	c->logdir         = sr_strdup(a, "log");
	if (srunlikely(c->logdir == NULL))
		return -1;
	c->logdir_read    = 1;
	c->logdir_write   = 1;
	c->logdir_create  = 1;
	c->dir            = NULL;
	c->dir_read       = 1;
	c->dir_write      = 1;
	c->dir_create     = 1;
	c->node_merge_wm  = 512;
	c->node_size      = 10;
	c->page_size      = 1024;
	c->threads        = 4;
	c->log_rotate_wm  = 50000;
	c->log_gc         = 1;
	c->db_rotate_wm   = 1000;
	c->db_gc          = 1;
	c->db_gc_factor   = 0.5;
	c->scheduler      = 1; /* off, parallel, on_commit */
	c->scheduler_tick = 1;
	c->snapshot       = 1;
	c->snapshot_wm    = 1000;

	c->node_merge_force_round = 5;
	c->node_merge_force_min   = 1;
	return 0;
}

int sr_conffree(srconf *c, sra *a)
{
	if (c->dir)
		sr_free(a, c->dir);
	if (c->logdir)
		sr_free(a, c->logdir);
	return 0;
}

int sr_confset(srconf *c, sra *a, char *path, va_list args)
{
	char q[200];
	snprintf(q, sizeof(q), "%s", path);
	char *ptr = NULL;
	char *token;
	token = strtok_r(q, ".", &ptr);
	if (srunlikely(token == NULL))
		return -1;
	if (strcmp(token, "env") != 0)
		return -1;
	token = strtok_r(NULL, ".", &ptr);
	if (srunlikely(token == NULL))
		return -1;
	if (strcmp(token, "alloc") == 0) {
		c->a = va_arg(args, srallocf);
	} else 
	if (strcmp(token, "alloc_arg") == 0) {
		c->aarg = va_arg(args, void*);
	} else 
	if (strcmp(token, "threads") == 0) {
		c->threads = va_arg(args, int);
	} else 
	if (strcmp(token, "dir") == 0) {
		char *p = sr_strdup(a, va_arg(args, char*));
		if (srunlikely(p == NULL))
			return -1;
		if (c->dir)
			sr_free(a, c->dir);
		c->dir = p;
	} else
	if (strcmp(token, "dir_read") == 0) {
		c->dir_read = va_arg(args, int);
	} else
	if (strcmp(token, "dir_write") == 0) {
		c->dir_write = va_arg(args, int);
	} else
	if (strcmp(token, "dir_create") == 0) {
		c->dir_create = va_arg(args, int);
	} else
	if (strcmp(token, "logdir") == 0) {
		char *p = sr_strdup(a, va_arg(args, char*));
		if (srunlikely(p == NULL))
			return -1;
		if (c->logdir)
			sr_free(a, c->logdir);
		c->logdir = p;
	} else
	if (strcmp(token, "logdir_read") == 0) {
		c->logdir_read = va_arg(args, int);
	} else
	if (strcmp(token, "logdir_write") == 0) {
		c->logdir_write = va_arg(args, int);
	} else
	if (strcmp(token, "logdir_create") == 0) {
		c->logdir_create = va_arg(args, int);
	} else
	if (strcmp(token, "page_size") == 0) {
		int page_size = va_arg(args, int);
		if (srunlikely((page_size % 2) != 0))
			return -1;
		c->page_size = page_size;
	} else
	if (strcmp(token, "node_size") == 0) {
		c->node_size = va_arg(args, int);
	} else
	if (strcmp(token, "node_merge_wm") == 0) {
		c->node_merge_wm = va_arg(args, int);
	} else
	if (strcmp(token, "log_rotate_wm") == 0) {
		c->log_rotate_wm = va_arg(args, int);
	} else
	if (strcmp(token, "log_gc") == 0) {
		c->log_gc = va_arg(args, int);
	} else
	if (strcmp(token, "db_rotate_wm") == 0) {
		c->db_rotate_wm = va_arg(args, int);
	} else
	if (strcmp(token, "db_gc") == 0) {
		c->db_gc = va_arg(args, int);
	} else
	if (strcmp(token, "scheduler") == 0) {
		c->scheduler = va_arg(args, int);
	} else
	if (strcmp(token, "scheduler_tick") == 0) {
		c->scheduler_tick = va_arg(args, int);
	} else
	if (strcmp(token, "snapshot") == 0) {
		c->snapshot = va_arg(args, int);
	} else
	if (strcmp(token, "snapshot_wm") == 0) {
		c->snapshot_wm = va_arg(args, int);
	} else {
		return -1;
	}
	return 0;
}

int sr_confvalidate(srconf *c)
{
	if (srunlikely(c->dir == NULL))
		return -1;
	if (srunlikely(c->logdir == NULL))
		return -1;
	return 0;
}
