
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <time.h>
#include <sys/time.h>
#include <string.h>

#include <sophia.h>
#include "test.h"

static void
test_gcoff_logrotate_empty(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	t( sp_set(c, "env.db_gc", 0) == 0 );
	t( sp_set(c, "env.log_gc", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./log", "0000.log") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");
	void *ctl = sp_ctl(db);
	t( ctl != NULL );
	t( sp_set(ctl, "logrotate") == 0 );
	t( exists("./log", "0000.log") == 1 );
	t( exists("./log", "0001.log") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
test_gcoff_logrotate(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	t( sp_set(c, "env.db_gc", 0) == 0 );
	t( sp_set(c, "env.log_gc", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./log", "0000.log") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "logrotate") == 0 );
	t( exists("./log", "0000.log") == 1 );
	t( exists("./log", "0001.log") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
test_gcoff_logrotate_gc0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	t( sp_set(c, "env.db_gc", 0) == 0 );
	t( sp_set(c, "env.log_gc", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./log", "0000.log") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( sp_set(sp_ctl(db), "logrotate") == 0 );
	t( exists("./log", "0000.log") == 1 );
	t( exists("./log", "0001.log") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
test_gcoff_logrotate_gc1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	t( sp_set(c, "env.db_gc", 0) == 0 );
	t( sp_set(c, "env.log_gc", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./log", "0000.log") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( sp_set(sp_ctl(db), "logrotate") == 0 );
	t( exists("./log", "0000.log") == 1 );
	t( exists("./log", "0001.log") == 1 );

	tx = sp_begin(db);
	t( tx != NULL );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "logrotate") == 0 );
	t( sp_set(sp_ctl(db), "logrotate") == 0 );
	t( sp_set(sp_ctl(db), "logrotate") == 0 );
	t( sp_set(sp_ctl(db), "logrotate") == 0 );

	t( exists("./log", "0001.log") == 1 );
	t( exists("./log", "0002.log") == 1 );
	t( exists("./log", "0003.log") == 1 );
	t( exists("./log", "0004.log") == 1 );
	t( exists("./log", "0005.log") == 1 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( sp_set(sp_ctl(db), "logrotate") == 0 );

	t( exists("./log", "0001.log") == 1 );
	t( exists("./log", "0005.log") == 1 );
	t( exists("./log", "0006.log") == 1 );

	t( sp_destroy(env) == 0 );
}

static void
test_gcoff_dbrotate_empty(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	t( sp_set(c, "env.db_gc", 0) == 0 );
	t( sp_set(c, "env.log_gc", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./test", "0000.db") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");
	void *ctl = sp_ctl(db);
	t( ctl != NULL );
	t( sp_set(ctl, "dbrotate") == 0 );
	t( exists("./test", "0000.db") == 1 );
	t( exists("./test", "0001.db") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
test_gcoff_dbrotate0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	t( sp_set(c, "env.db_gc", 0) == 0 );
	t( sp_set(c, "env.log_gc", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./test", "0000.db") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( sp_set(sp_ctl(db), "dbrotate") == 0 );
	t( exists("./test", "0000.db") == 1 );
	t( exists("./test", "0001.db") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
test_gcoff_dbrotate1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	t( sp_set(c, "env.db_gc", 0) == 0 );
	t( sp_set(c, "env.log_gc", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./test", "0000.db") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( sp_set(sp_ctl(db), "dbrotate") == 0 );
	t( exists("./test", "0000.db") == 1 );
	t( exists("./test", "0001.db") == 1 );

	tx = sp_begin(db);
	t( tx != NULL );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( sp_set(sp_ctl(db), "dbrotate") == 0 );

	t( exists("./test", "0000.db") == 1 );
	t( exists("./test", "0001.db") == 1 );
	t( exists("./test", "0002.db") == 1 );

	t( sp_destroy(env) == 0 );
}

static void
test_logrotate_empty(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./log", "0000.log") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");
	void *ctl = sp_ctl(db);
	t( ctl != NULL );
	t( sp_set(ctl, "logrotate") == 0 );
	t( exists("./log", "0000.log") == 0 ); /* scheme creation, forced merge */
	t( exists("./log", "0001.log") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
test_logrotate(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./log", "0000.log") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "logrotate") == 0 );
	t( exists("./log", "0000.log") == 1 );
	t( exists("./log", "0001.log") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
test_logrotate_gc0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./log", "0000.log") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( sp_set(sp_ctl(db), "logrotate") == 0 );
	t( exists("./log", "0000.log") == 0 ); /* scheme creation, forced merge */
	t( exists("./log", "0001.log") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
test_logrotate_gc1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./log", "0000.log") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( sp_set(sp_ctl(db), "logrotate") == 0 );
	t( exists("./log", "0000.log") == 0 ); /* scheme creation, forced merge */
	t( exists("./log", "0001.log") == 1 );

	tx = sp_begin(db);
	t( tx != NULL );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "logrotate") == 0 );
	t( sp_set(sp_ctl(db), "logrotate") == 0 );
	t( sp_set(sp_ctl(db), "logrotate") == 0 );
	t( sp_set(sp_ctl(db), "logrotate") == 0 );

	t( exists("./log", "0001.log") == 1 );
	t( exists("./log", "0002.log") == 0 );
	t( exists("./log", "0003.log") == 0 );
	t( exists("./log", "0004.log") == 0 );
	t( exists("./log", "0005.log") == 1 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( sp_set(sp_ctl(db), "logrotate") == 0 );

	t( exists("./log", "0001.log") == 0 );
	t( exists("./log", "0005.log") == 0 );
	t( exists("./log", "0006.log") == 1 );

	t( sp_destroy(env) == 0 );
}

static void
test_dbrotate_empty(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./test", "0000.db") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");
	void *ctl = sp_ctl(db);
	t( ctl != NULL );
	t( sp_set(ctl, "dbrotate") == 0 );
	t( exists("./test", "0000.db") == 1 ); /* scheme */
	t( exists("./test", "0001.db") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
test_dbrotate0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./test", "0000.db") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( sp_set(sp_ctl(db), "dbrotate") == 0 );
	t( exists("./test", "0000.db") == 1 );
	t( exists("./test", "0001.db") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
test_dbrotate1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./test", "0000.db") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	t( sp_set(sp_ctl(db), "dbrotate") == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( exists("./test", "0000.db") == 1 ); /* scheme */
	t( exists("./test", "0001.db") == 1 );
	t( sp_destroy(env) == 0 );
}

static void
test_dbrotate2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("./test", "0000.db") == 1 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( sp_set(sp_ctl(db), "dbrotate") == 0 );
	t( exists("./test", "0000.db") == 1 );
	t( exists("./test", "0001.db") == 1 );

	tx = sp_begin(db);
	t( tx != NULL );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( sp_set(sp_ctl(db), "dbrotate") == 0 );

	t( exists("./test", "0000.db") == 1 ); /* scheme */
	t( exists("./test", "0001.db") == 1 );
	t( exists("./test", "0002.db") == 1 );

	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test_gcoff_logrotate_empty );
	test( test_gcoff_logrotate );
	test( test_gcoff_logrotate_gc0 );
	test( test_gcoff_logrotate_gc1 );
	test( test_gcoff_dbrotate_empty );
	test( test_gcoff_dbrotate0 );
	test( test_gcoff_dbrotate1 );

	test( test_logrotate_empty );
	test( test_logrotate );
	test( test_logrotate_gc0 );
	test( test_logrotate_gc1 );
	test( test_dbrotate_empty );
	test( test_dbrotate0 );
	test( test_dbrotate1 );
	test( test_dbrotate2 );
	return 0;
}
