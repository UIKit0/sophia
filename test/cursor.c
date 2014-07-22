
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

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>
#include <sophia.h>
#include "test.h"

static void
test_empty_gte(void)
{
	rmrf("./test");
	rmrf("./log");
	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");
	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 0 );
	t( sp_key(c) == NULL );
	t( sp_keysize(c) == 0 );
	t( sp_value(c) == NULL );
	t( sp_valuesize(c) == 0 );
	t( sp_destroy(c) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_empty_gt(void)
{
	rmrf("./test");
	rmrf("./log");
	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");
	void *c = sp_cursor(db, ">", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 0 );
	t( sp_key(c) == NULL );
	t( sp_keysize(c) == 0 );
	t( sp_value(c) == NULL );
	t( sp_valuesize(c) == 0 );
	t( sp_destroy(c) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_empty_lte(void)
{
	rmrf("./test");
	rmrf("./log");
	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");
	void *c = sp_cursor(db, "<=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 0 );
	t( sp_key(c) == NULL );
	t( sp_keysize(c) == 0 );
	t( sp_value(c) == NULL );
	t( sp_valuesize(c) == 0 );
	t( sp_destroy(c) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_empty_lt(void)
{
	rmrf("./test");
	rmrf("./log");
	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");
	void *c = sp_cursor(db, "<", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 0 );
	t( sp_key(c) == NULL );
	t( sp_keysize(c) == 0 );
	t( sp_value(c) == NULL );
	t( sp_valuesize(c) == 0 );
	t( sp_destroy(c) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_gte(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_gt(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, ">", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_lte(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, "<=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_lt(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, "<", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 7;
	void *c = sp_cursor(db, ">=", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 8;
	void *c = sp_cursor(db, ">=", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 9;
	void *c = sp_cursor(db, ">=", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte3(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 15;
	void *c = sp_cursor(db, ">=", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte4(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 73;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 80;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 90;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 79;
	void *c = sp_cursor(db, ">=", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 80 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 90 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte5(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 0;
	void *c = sp_cursor(db, ">=", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gt0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 7;
	void *c = sp_cursor(db, ">", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gt1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 8;
	void *c = sp_cursor(db, ">", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gt2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 9;
	void *c = sp_cursor(db, ">", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 9;
	void *c = sp_cursor(db, "<=", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 8;
	void *c = sp_cursor(db, "<=", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 7;
	void *c = sp_cursor(db, "<=", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte3(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 5;
	void *c = sp_cursor(db, "<=", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte4(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 20;
	void *c = sp_cursor(db, "<=", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lt0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 9;
	void *c = sp_cursor(db, "<", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lt1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 8;
	void *c = sp_cursor(db, "<", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lt2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 7;
	void *c = sp_cursor(db, "<", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lt3(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 2;
	void *c = sp_cursor(db, "<", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_lt4(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 20;
	void *c = sp_cursor(db, "<", &key, sizeof(key));
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte_range(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
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
	int i = 0;
	while (i < 385) {
		t( sp_set(tx, &i, sizeof(i), &i, sizeof(i)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	i = 0;
	while (i < 385) {
		void *c = sp_cursor(db, ">=", &i, sizeof(i));
		t( c != NULL );
		t( sp_fetch(c) == 1 );
		t( *(int*)sp_key(c) == i );
		t( *(int*)sp_value(c) == i );
		t( sp_destroy(c) == 0 );
		i++;
	}
	t( sp_destroy(env) == 0 );
}

static void
test_pos_gt_range(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
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
	int i = 0;
	while (i < 385) {
		t( sp_set(tx, &i, sizeof(i), &i, sizeof(i)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	i = 0;
	while (i < (385 - 1)) {
		void *c = sp_cursor(db, ">", &i, sizeof(i));
		t( c != NULL );
		t( sp_fetch(c) == 1 );
		t( *(int*)sp_key(c) == (i + 1));
		t( *(int*)sp_value(c) == (i + 1));
		t( sp_destroy(c) == 0 );
		i++;
	}
	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte_range(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
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
	int i = 0;
	while (i < 385) {
		t( sp_set(tx, &i, sizeof(i), &i, sizeof(i)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	i = 0;
	while (i < 385) {
		void *c = sp_cursor(db, "<=", &i, sizeof(i));
		t( c != NULL );
		t( sp_fetch(c) == 1 );
		t( *(int*)sp_key(c) == i );
		t( *(int*)sp_value(c) == i );
		t( sp_destroy(c) == 0 );
		i++;
	}
	t( sp_destroy(env) == 0 );
}

static void
test_pos_lt_range(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
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
	int i = 0;
	while (i < 385) {
		t( sp_set(tx, &i, sizeof(i), &i, sizeof(i)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	i = 1;
	while (i < 385) {
		void *c = sp_cursor(db, "<", &i, sizeof(i));
		t( c != NULL );
		t( sp_fetch(c) == 1 );
		t( *(int*)sp_key(c) == i - 1 );
		t( *(int*)sp_value(c) == i - 1 );
		t( sp_destroy(c) == 0 );
		i++;
	}
	t( sp_destroy(env) == 0 );
}

static void
test_pos_gte_random(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	unsigned int seed = time(NULL);
	srand(seed);

	void *tx = sp_begin(db);
	t( tx != NULL );
	int i = 0;
	while (i < 270) {
		int key = rand();
		t( sp_set(tx, &key, sizeof(key), &i, sizeof(i)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );

	srand(seed);
	i = 0;
	while (i < 270) {
		int key = rand();
		void *c = sp_cursor(db, ">=", &key, sizeof(key));
		t( c != NULL );
		t( sp_fetch(c) == 1 );
		t( *(int*)sp_key(c) == key );
		t( *(int*)sp_value(c) == i );
		t( sp_destroy(c) == 0 );
		i++;
	}
	t( sp_destroy(env) == 0 );
}

static void
test_pos_lte_random(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	unsigned int seed = time(NULL);
	srand(seed);

	void *tx = sp_begin(db);
	t( tx != NULL );
	int i = 0;
	while (i < 403) {
		int key = rand();
		t( sp_set(tx, &key, sizeof(key), &i, sizeof(i)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );

	srand(seed);
	i = 0;
	while (i < 403) {
		int key = rand();
		void *c = sp_cursor(db, "<=", &key, sizeof(key));
		t( c != NULL );
		t( sp_fetch(c) == 1 );
		t( *(int*)sp_key(c) == key );
		t( *(int*)sp_value(c) == i );
		t( sp_destroy(c) == 0 );
		i++;
	}
	t( sp_destroy(env) == 0 );
}

static void
test_consistency_0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int key;
	void *tx = sp_begin(db);
	t( tx != NULL );
	key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );

	tx = sp_begin(db);
	t( tx != NULL );
	key = 0;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 19;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int k;
	int v = 2;
	void *tx = sp_begin(db);
	t( tx != NULL );
	k = 1;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 1;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 2 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 3 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_3(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int k;
	int v = 2;
	void *tx = sp_begin(db);
	t( tx != NULL );
	k = 1;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 1;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 2 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 3 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_4(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int k;
	int v = 2;
	void *tx = sp_begin(db);
	t( tx != NULL );
	k = 1;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 1;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 3;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 4;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 2 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 3 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_5(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int k;
	int v = 2;
	void *tx = sp_begin(db);
	t( tx != NULL );
	k = 1;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 4;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 6;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 2 );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 2;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 4 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 6 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_6(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int k;
	int v = 2;
	void *tx = sp_begin(db);
	t( tx != NULL );
	k = 1;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 4;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 6;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 2 );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 2;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 4;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 4 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 6 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_7(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int k;
	int v = 2;
	void *tx = sp_begin(db);
	t( tx != NULL );
	k = 1;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 4;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 6;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 2 );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 0;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 4;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 5;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 6;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 7;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 4 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 6 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_8(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int k;
	int v = 2;
	void *tx = sp_begin(db);
	t( tx != NULL );
	k = 1;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 4;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 6;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 2 );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 0;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 4;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 4 );
	t( *(int*)sp_value(c) == 2 );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 0;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 5;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 6;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 6 );
	t( *(int*)sp_value(c) == 2 );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 7;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_9(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int k;
	int v = 2;
	void *tx = sp_begin(db);
	t( tx != NULL );
	k = 1;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 4;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 6;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 2 );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 0;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 4;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	tx = sp_begin(db);
	t( tx != NULL );
	k = 4;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 4 );
	t( *(int*)sp_value(c) == 2 );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 5;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 6;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 6 );
	t( *(int*)sp_value(c) == 2 );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 7;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 8;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 0 );
	t( *(int*)sp_value(c) == 3 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 2 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 4 );
	t( *(int*)sp_value(c) == 3 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 5 );
	t( *(int*)sp_value(c) == 3 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 6 );
	t( *(int*)sp_value(c) == 3 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( *(int*)sp_value(c) == 3 );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( *(int*)sp_value(c) == 3 );
	
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_n(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	int k;
	int v = 2;
	void *tx = sp_begin(db);
	t( tx != NULL );
	k = 1;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 2;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 2 );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 0;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	k = 4;
	v = 3;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	tx = sp_begin(db);
	t( tx != NULL );
	k = 4;
	t( sp_set(tx, &k, sizeof(k), &v, sizeof(v)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *c2 = sp_cursor(db, ">=", NULL, 0);
	t( c2 != NULL );

	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 2 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 3 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );

	t( sp_fetch(c2) == 1 );
	t( *(int*)sp_key(c2) == 0 );
	t( *(int*)sp_value(c2) == 2 );
	t( sp_fetch(c2) == 1 );
	t( *(int*)sp_key(c2) == 1 );
	t( *(int*)sp_value(c2) == 2 );
	t( sp_fetch(c2) == 1 );
	t( *(int*)sp_key(c2) == 2 );
	t( *(int*)sp_value(c2) == 2 );
	t( sp_fetch(c2) == 1 );
	t( *(int*)sp_key(c2) == 3 );
	t( *(int*)sp_value(c2) == 2 );
	t( sp_fetch(c2) == 1 );
	t( *(int*)sp_key(c2) == 4 );
	t( *(int*)sp_value(c2) == 3 );
	t( sp_fetch(c2) == 0 );
	t( sp_destroy(c2) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_rewrite0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *c0 = sp_cursor(db, ">=", NULL, 0);

	void *tx = sp_begin(db);
	t( tx != NULL );
	int v = 15;
	int i = 0;
	while (i < 385) {
		t( sp_set(tx, &i, sizeof(i), &v, sizeof(v)) == 0 );
		i++;
	}
	rc = sp_commit(tx);

	void *c1 = sp_cursor(db, ">=", NULL, 0);

	tx = sp_begin(db);
	v = 20;
	i = 0;
	while (i < 385) {
		t( sp_set(tx, &i, sizeof(i), &v, sizeof(v)) == 0 );
		i++;
	}
	rc = sp_commit(tx);

	void *c2 = sp_cursor(db, ">=", NULL, 0);

	t( sp_fetch(c0) == 0 );

	i = 0;
	while (sp_fetch(c1)) {
		t( *(int*)sp_key(c1) == i );
		t( *(int*)sp_value(c1) == 15 );
		i++;
	}
	t(i == 385);

	i = 0;
	while (sp_fetch(c2)) {
		t( *(int*)sp_key(c2) == i );
		t( *(int*)sp_value(c2) == 20 );
		i++;
	}
	t(i == 385);

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(c2) == 0 );
	t( sp_destroy(c1) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_rewrite1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *c0 = sp_cursor(db, ">=", NULL, 0);

	void *tx = sp_begin(db);
	t( tx != NULL );
	int v = 15;
	int i = 0;
	while (i < 385) {
		t( sp_set(tx, &i, sizeof(i), &v, sizeof(v)) == 0 );
		i++;
	}
	rc = sp_commit(tx);

	void *c1 = sp_cursor(db, ">=", NULL, 0);

	tx = sp_begin(db);
	v = 20;
	i = 0;
	while (i < 385) {
		t( sp_set(tx, &i, sizeof(i), &v, sizeof(v)) == 0 );
		i++;
	}
	rc = sp_commit(tx);

	t( sp_fetch(c0) == 0 );

	i = 0;
	while (sp_fetch(c1)) {
		t( *(int*)sp_key(c1) == i );
		t( *(int*)sp_value(c1) == 15 );
		i++;
	}
	t(i == 385);

	i = 0;
	while (i < 385) {
		void *c2 = sp_cursor(db, ">=", &i, sizeof(i));
		t( *(int*)sp_key(c2) == i );
		t( *(int*)sp_value(c2) == 20 );
		t( sp_destroy(c2) == 0 );
		i++;
	}
	t(i == 385);

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(c1) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_consistency_rewrite2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	void *db = sp_use(env, "test");

	void *c0 = sp_cursor(db, ">=", NULL, 0);

	void *tx = sp_begin(db);
	t( tx != NULL );
	int v = 15;
	int i = 0;
	while (i < 385) {
		t( sp_set(tx, &i, sizeof(i), &v, sizeof(v)) == 0 );
		i++;
	}
	rc = sp_commit(tx);

	void *c1 = sp_cursor(db, ">=", NULL, 0);

	v = 20;
	i = 0;
	while (sp_fetch(c1)) {
		t( *(int*)sp_key(c1) == i );
		t( *(int*)sp_value(c1) == 15 );
		tx = sp_begin(db);
		t( sp_set(tx, &i, sizeof(i), &v, sizeof(v)) == 0 );
		t( sp_commit(tx) == 0 );
		i++;
	}
	t(i == 385);

	t( sp_fetch(c0) == 0 );

	void *c2 = sp_cursor(db, ">=", NULL, 0);
	i = 0;
	while (sp_fetch(c2)) {
		t( *(int*)sp_key(c2) == i );
		t( *(int*)sp_value(c2) == 20 );
		i++;
	}
	t(i == 385);

	t( sp_destroy(c0) == 0 );
	t( sp_destroy(c2) == 0 );
	t( sp_destroy(c1) == 0 );

	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test_empty_gte );
	test( test_empty_gt );
	test( test_empty_lte );
	test( test_empty_lt );
	test( test_gte );
	test( test_gt );
	test( test_lte );
	test( test_lt );
	test( test_pos_gte0 );
	test( test_pos_gte1 );
	test( test_pos_gte2 );
	test( test_pos_gte3 );
	test( test_pos_gte4 );
	test( test_pos_gte5 );
	test( test_pos_gt0 );
	test( test_pos_gt1 );
	test( test_pos_gt2 );
	test( test_pos_lte0 );
	test( test_pos_lte1 );
	test( test_pos_lte2 );
	test( test_pos_lte3 );
	test( test_pos_lte4 );
	test( test_pos_lt0 );
	test( test_pos_lt1 );
	test( test_pos_lt2 );
	test( test_pos_lt3 );
	test( test_pos_lt4 );
	test( test_pos_gte_range );
	test( test_pos_gt_range );
	test( test_pos_lte_range );
	test( test_pos_lt_range );
	test( test_pos_gte_random );
	test( test_pos_lte_random );
	test( test_consistency_0 );
	test( test_consistency_1 );
	test( test_consistency_2 );
	test( test_consistency_3 );
	test( test_consistency_4 );
	test( test_consistency_5 );
	test( test_consistency_6 );
	test( test_consistency_7 );
	test( test_consistency_8 );
	test( test_consistency_9 );
	test( test_consistency_n );
	test( test_consistency_rewrite0 );
	test( test_consistency_rewrite1 );
	test( test_consistency_rewrite2 );

	/* merge */
	/* merge multi-node */
	/* merge multi-node multi-db */
	return 0;
}
