
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
#include <sophia.h>
#include "test.h"

static void
test_recover_log_empty(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	rc = sp_open(env);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_recover_log_set_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );

	void *test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	t( sp_open(test) == 0 );

	void *tx = sp_begin(test);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );

	/* declare scheme, do not open */
	test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );

	rc = sp_open(env);
	t( rc == 0 );

	tx = sp_begin(test);
	void *value = NULL;
	int valuesize = 0;
	t( sp_get(tx, &key, sizeof(key), &value, &valuesize) == 1 );
	t( *(int*)value == key );
	free(value);
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_recover_log_set_get_n(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );

	void *test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	t( sp_open(test) == 0 );

	void *tx = sp_begin(test);
	t( tx != NULL );
	int i = 0;
	while (i < 941) {
		t( sp_set(tx, &i, sizeof(i), &i, sizeof(i)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_destroy(env) == 0 );


	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );

	test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	rc = sp_open(env);
	t( rc == 0 );

	tx = sp_begin(test);
	i = 0;
	while (i < 941) {
		void *value = NULL;
		int valuesize = 0;
		t( sp_get(tx, &i, sizeof(i), &value, &valuesize) == 1 );
		t( *(int*)value == i );
		free(value);
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_recover_log_replace_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );

	void *test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	t( sp_open(test) == 0 );

	void *tx = sp_begin(test);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );

	tx = sp_begin(test);
	t( tx != NULL );
	key = 7;
	int value = 8;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );

	test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );

	rc = sp_open(env);
	t( rc == 0 );

	tx = sp_begin(test);
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 8 );
	free(v);
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_recover_log_replace_get_n(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );

	void *test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	t( sp_open(test) == 0 );

	void *tx = sp_begin(test);
	t( tx != NULL );
	int v = 7;
	int i = 0;
	while (i < 597) {
		t( sp_set(tx, &i, sizeof(i), &v, sizeof(v)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );

	tx = sp_begin(test);
	t( tx != NULL );
	v = 8;
	i = 0;
	while (i < 597) {
		t( sp_set(tx, &i, sizeof(i), &v, sizeof(v)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );

	t( sp_destroy(test) == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );

	test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );

	rc = sp_open(env);
	t( rc == 0 );

	tx = sp_begin(test);
	i = 0;
	while (i < 597) {
		void *value = NULL;
		int valuesize = 0;
		t( sp_get(tx, &i, sizeof(i), &value, &valuesize) == 1 );
		t( *(int*)value == 8 );
		free(value);
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_recover_log_set_replace_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );

	void *test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	t( sp_open(test) == 0 );

	void *tx = sp_begin(test);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_destroy(test) == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );

	test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	rc = sp_open(env);
	t( rc == 0 );

	tx = sp_begin(test);
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 7 );
	free(v);
	rc = sp_commit(tx);
	t( rc == 0 );

	tx = sp_begin(test);
	t( tx != NULL );
	key = 7;
	int value = 8;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );

	test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	rc = sp_open(env);
	t( rc == 0 );

	tx = sp_begin(test);
	v = NULL;
	vsize = 0;
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 8 );
	free(v);
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_recover_log_fetch_lte(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );

	void *test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	t( sp_open(test) == 0 );

	void *tx = sp_begin(test);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );

	test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	rc = sp_open(env);
	t( rc == 0 );

	void *cur = sp_cursor(test, "<=", NULL, 0);
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 9 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 8 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 7 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_recover_log_fetch_gte(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );

	void *test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	t( sp_open(test) == 0 );

	void *tx = sp_begin(test);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_destroy(test) == 0 );

	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );

	test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );

	rc = sp_open(env);
	t( rc == 0 );

	void *cur = sp_cursor(test, ">=", NULL, 0);
	t( cur != NULL );

	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 7 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 8 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 9 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_recover_merge_empty(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );

	void *test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	t( sp_open(test) == 0 );
	t( sp_set(sp_ctl(test, "ctl"), "merge") == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	rc = sp_open(env);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_recover_merge_set_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );

	void *test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	t( sp_open(test) == 0 );

	void *tx = sp_begin(test);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_set(sp_ctl(test, "ctl"), "merge") == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	rc = sp_open(env);
	t( rc == 0 );

	tx = sp_begin(test);
	void *value = NULL;
	int valuesize = 0;
	t( sp_get(tx, &key, sizeof(key), &value, &valuesize) == 1 );
	t( *(int*)value == key );
	free(value);
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_recover_merge_set_get_n(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );

	void *test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	t( sp_open(test) == 0 );

	void *tx = sp_begin(test);
	t( tx != NULL );
	int i = 0;
	while (i < 831) {
		t( sp_set(tx, &i, sizeof(i), &i, sizeof(i)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_set(sp_ctl(test, "ctl"), "merge") == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	rc = sp_open(env);
	t( rc == 0 );
	tx = sp_begin(test);
	i = 0;
	while (i < 831) {
		void *value = NULL;
		int valuesize = 0;
		t( sp_get(tx, &i, sizeof(i), &value, &valuesize) == 1 );
		t( *(int*)value == i );
		free(value);
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_recover_merge_replace_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );

	void *test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	t( sp_open(test) == 0 );

	void *tx = sp_begin(test);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_set(sp_ctl(test, "ctl"), "merge") == 0 );

	tx = sp_begin(test);
	t( tx != NULL );
	key = 7;
	int value = 8;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_set(sp_ctl(test, "ctl"), "merge") == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	rc = sp_open(env);
	t( rc == 0 );

	tx = sp_begin(test);
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 8 );
	free(v);
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_recover_merge_replace_get_n(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );

	void *test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	t( sp_open(test) == 0 );

	void *tx = sp_begin(test);
	t( tx != NULL );
	int v = 7;
	int i = 0;
	while (i < 431) {
		t( sp_set(tx, &i, sizeof(i), &v, sizeof(v)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_set(sp_ctl(test, "ctl"), "merge") == 0 );

	tx = sp_begin(test);
	t( tx != NULL );
	v = 8;
	i = 0;
	while (i < 431) {
		t( sp_set(tx, &i, sizeof(i), &v, sizeof(v)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_set(sp_ctl(test, "ctl"), "merge") == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	rc = sp_open(env);
	t( rc == 0 );
	tx = sp_begin(test);
	i = 0;
	while (i < 431) {
		void *value = NULL;
		int valuesize = 0;
		t( sp_get(tx, &i, sizeof(i), &value, &valuesize) == 1 );
		t( *(int*)value == 8 );
		free(value);
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

#if 0
static void
test_recover_merge_set_replace_get(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_set(sp_ctl(scheme), "merge") == 0 );
	t( sp_destroy(scheme) == 0 );
	void *test = sp_use(env, "test");
	void *tx = sp_begin(test);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_set(sp_ctl(test), "merge") == 0 );
	t( sp_destroy(test) == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	rc = sp_open(env);
	t( rc == 0 );
	test = sp_use(env, "test");
	t( test != NULL );
	tx = sp_begin(test);
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 7 );
	free(v);
	rc = sp_commit(tx);
	t( rc == 0 );

	tx = sp_begin(test);
	t( tx != NULL );
	key = 7;
	int value = 8;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_set(sp_ctl(test), "merge") == 0 );
	t( sp_destroy(test) == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	rc = sp_open(env);
	t( rc == 0 );
	test = sp_use(env, "test");
	t( test != NULL );
	tx = sp_begin(test);
	v = NULL;
	vsize = 0;
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 8 );
	free(v);
	t( sp_destroy(test) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_recover_merge_fetch_lte(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_set(sp_ctl(scheme), "merge") == 0 );
	t( sp_destroy(scheme) == 0 );

	void *test = sp_use(env, "test");
	void *tx = sp_begin(test);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_set(sp_ctl(test), "merge") == 0 );
	t( sp_destroy(test) == 0 );

	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	rc = sp_open(env);
	t( rc == 0 );
	test = sp_use(env, "test");

	void *cur = sp_cursor(test, "<=", NULL, 0);
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 9 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 8 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 7 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );

	t( sp_destroy(test) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_recover_merge_fetch_gte(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_set(sp_ctl(scheme), "merge") == 0 );
	t( sp_destroy(scheme) == 0 );

	void *test = sp_use(env, "test");
	void *tx = sp_begin(test);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_set(sp_ctl(test), "merge") == 0 );
	t( sp_destroy(test) == 0 );

	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	rc = sp_open(env);
	t( rc == 0 );
	test = sp_use(env, "test");

	void *cur = sp_cursor(test, ">=", NULL, 0);
	t( cur != NULL );

	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 7 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 8 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 9 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );

	t( sp_destroy(test) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_recover_merge_log_fetch_lte(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_set(sp_ctl(scheme), "merge") == 0 );
	t( sp_destroy(scheme) == 0 );

	void *test = sp_use(env, "test");
	void *tx = sp_begin(test);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_set(sp_ctl(test), "merge") == 0 );

	tx = sp_begin(test);
	t( tx != NULL );
	key = 10;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 11;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 12;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(test) == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	rc = sp_open(env);
	t( rc == 0 );
	test = sp_use(env, "test");

	void *cur = sp_cursor(test, "<=", NULL, 0);
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 12 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 11 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 10 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 9 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 8 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 7 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );

	t( sp_destroy(test) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_recover_merge_log_fetch_gte(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );
	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_set(sp_ctl(scheme), "merge") == 0 );
	t( sp_destroy(scheme) == 0 );

	void *test = sp_use(env, "test");
	void *tx = sp_begin(test);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( test != NULL );
	t( sp_set(sp_ctl(test), "merge") == 0 );

	tx = sp_begin(test);
	t( tx != NULL );
	key = 10;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 11;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 12;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(test) == 0 );
	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	rc = sp_open(env);
	t( rc == 0 );
	test = sp_use(env, "test");

	void *cur = sp_cursor(test, ">=", NULL, 0);
	t( cur != NULL );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 7 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 8 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 9 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 10 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 11 );
	t( sp_fetch(cur) == 1 );
	t( *(int*)sp_key(cur) == 12 );
	t( sp_fetch(cur) == 0 );
	t( sp_destroy(cur) == 0 );

	t( sp_destroy(test) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_recover_drop_empty(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );

	void *scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	void *stx = sp_begin(scheme);
	t( sp_set(stx, "test") == 0);
	t( sp_set(stx, "test.cmp", "u32") == 0 );
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	scheme = sp_use(env, "scheme");
	t( scheme != NULL );
	stx = sp_begin(scheme);
	t( sp_delete(stx, "test") == 0);
	t( sp_commit(stx) == 0 );
	t( sp_destroy(scheme) == 0 );

	t( sp_destroy(env) == 0 );

	env = sp_env();
	t( env != NULL );
	c = sp_use(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	sp_destroy(c);
	rc = sp_open(env);
	t( rc == 0 );

	void *test = sp_use(env, "test");
	t( test == NULL );
	t( sp_destroy(env) == 0 );
}
#endif

int
main(int argc, char *argv[])
{
	test( test_recover_log_empty );
	test( test_recover_log_set_get );
	test( test_recover_log_set_get_n );
	test( test_recover_log_replace_get );
	test( test_recover_log_replace_get_n );
	test( test_recover_log_set_replace_get );
	test( test_recover_log_fetch_lte );
	test( test_recover_log_fetch_gte );

	test( test_recover_merge_empty );
	test( test_recover_merge_set_get );
	test( test_recover_merge_set_get_n );
	test( test_recover_merge_replace_get );
	test( test_recover_merge_replace_get_n );
#if 0
	test( test_recover_merge_set_replace_get );
	test( test_recover_merge_fetch_lte );
	test( test_recover_merge_fetch_gte );
	test( test_recover_merge_log_fetch_lte );
	test( test_recover_merge_log_fetch_gte );
	test( test_recover_drop_empty );
#endif
	return 0;
}
