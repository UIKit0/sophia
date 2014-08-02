
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <sophia.h>
#include "test.h"

static void
test_rollback(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	rc = sp_rollback(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_commit(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_commit(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_get_commit(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
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
test_set_commit_get0(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	tx = sp_begin(db);
	void *value = NULL;
	int valuesize = 0;
	t( sp_get(tx, &key, sizeof(key), &value, &valuesize) == 1 );
	t( *(int*)value == key );
	free(value);
	rc = sp_rollback(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_commit_get1(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	
	int key = 0;
	while (key < 10) {
		t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
		key++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );

	key = 0;
	tx = sp_begin(db);
	while (key < 10) {
		void *value = NULL;
		int valuesize = 0;
		t( sp_get(tx, &key, sizeof(key), &value, &valuesize) == 1 );
		t( *(int*)value == key );
		free(value);
		key++;
	}
	rc = sp_rollback(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_rollback(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_rollback(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_rollback_get0(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	rc = sp_rollback(tx);
	t( rc == 0 );
	tx = sp_begin(db);
	void *value = NULL;
	int valuesize = 0;
	t( sp_get(tx, &key, sizeof(key), &value, &valuesize) == 0 );
	rc = sp_rollback(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_rollback_get1(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 0;
	while (key < 10) {
		t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
		key++;
	}
	rc = sp_rollback(tx);
	t( rc == 0 );
	tx = sp_begin(db);
	key = 0;
	while (key < 10) {
		void *value = NULL;
		int valuesize = 0;
		t( sp_get(tx, &key, sizeof(key), &value, &valuesize) == 0 );
		key++;
	}
	rc = sp_rollback(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_set_commit(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	value = 8;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_set_get_commit(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	value = 8;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == value );
	free(v);
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_set_commit_get(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	value = 8;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	tx = sp_begin(db);
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == value );
	rc = sp_rollback(tx);
	t( rc == 0 );
	free(v);
	t( sp_destroy(env) == 0 );
}

static void
test_set_set_rollback_get(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	value = 8;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_rollback(tx);
	t( rc == 0 );
	tx = sp_begin(db);
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 0 );
	rc = sp_rollback(tx);
	t( rc == 0 );
	free(v);
	t( sp_destroy(env) == 0 );
}

static void
test_set_delete_get_commit(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_delete(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_delete_get_commit_get(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_delete(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_get(db, &key, sizeof(key), &v, &vsize) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_set_delete_set_commit_get(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_delete(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	value = 8;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == value );
	free(v);
	rc = sp_commit(tx);
	t( rc == 0 );
	t( sp_get(db, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == value );
	free(v);

	t( sp_destroy(env) == 0 );
}

static void
test_set_delete_commit_get_set(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	int key = 7;
	int value = key;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_delete(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );
	void *v = NULL;
	int vsize = 0;
	t( sp_get(db, &key, sizeof(key), &v, &vsize) == 0 );
	t( sp_set(db, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_get(db, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == value );
	free(v);
	t( sp_destroy(env) == 0 );
}

static void
test_p_set_commit(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key_a = 7;
	t( sp_set(a, &key_a, sizeof(key_a), &value, sizeof(value)) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	int key_b = 8;
	t( sp_set(b, &key_b, sizeof(key_b), &value, sizeof(value)) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_get_commit(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value_a = 10;
	int key_a = 7;
	t( sp_set(a, &key_a, sizeof(key_a), &value_a, sizeof(value_a)) == 0 );
	void *v = NULL;
	int vsize = 0;
	t( sp_get(a, &key_a, sizeof(key_a), &v, &vsize) == 1 );
	t( *(int*)v == value_a );
	free(v);
	rc = sp_commit(a);

	t( rc == 0 );
	int value_b = 15;
	int key_b = 8;
	t( sp_set(b, &key_b, sizeof(key_b), &value_b, sizeof(value_b)) == 0 );
	v = NULL;
	vsize = 0;
	t( sp_get(b, &key_b, sizeof(key_b), &v, &vsize) == 1 );
	t( *(int*)v == value_b );
	free(v);
	rc = sp_commit(b);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_commit_get0(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value_a = 10;
	int key_a = 7;
	t( sp_set(a, &key_a, sizeof(key_a), &value_a, sizeof(value_a)) == 0 );
	rc = sp_commit(a);

	t( rc == 0 );
	int value_b = 15;
	int key_b = 8;
	t( sp_set(b, &key_b, sizeof(key_b), &value_b, sizeof(value_b)) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );

	void *tx = sp_begin(db);
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key_a, sizeof(key_a), &v, &vsize) == 1 );
	t( *(int*)v == value_a );
	free(v);
	t( sp_get(tx, &key_b, sizeof(key_b), &v, &vsize) == 1 );
	t( *(int*)v == value_b );
	free(v);
	t( sp_rollback(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_commit_get1(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );

	int value_a = 10;
	int key_a = 7;
	t( sp_set(a, &key_a, sizeof(key_a), &value_a, sizeof(value_a)) == 0 );
	rc = sp_commit(a);

	t( rc == 0 );
	int value_b = 15;
	int key_b = 8;
	t( sp_set(b, &key_b, sizeof(key_b), &value_b, sizeof(value_b)) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );

	void *tx = sp_begin(db);
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key_a, sizeof(key_a), &v, &vsize) == 1 );
	t( *(int*)v == value_a );
	free(v);
	t( sp_get(tx, &key_b, sizeof(key_b), &v, &vsize) == 1 );
	t( *(int*)v == value_b );
	free(v);
	t( sp_rollback(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_commit_get2(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );

	t( rc == 0 );
	int value_b = 15;
	int key_b = 8;
	t( sp_set(b, &key_b, sizeof(key_b), &value_b, sizeof(value_b)) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );

	int value_a = 10;
	int key_a = 7;
	t( sp_set(a, &key_a, sizeof(key_a), &value_a, sizeof(value_a)) == 0 );
	rc = sp_commit(a);

	void *tx = sp_begin(db);
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key_a, sizeof(key_a), &v, &vsize) == 1 );
	t( *(int*)v == value_a );
	free(v);
	t( sp_get(tx, &key_b, sizeof(key_b), &v, &vsize) == 1 );
	t( *(int*)v == value_b );
	free(v);
	t( sp_rollback(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_rollback_get0(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value_a = 10;
	int key_a = 7;
	t( sp_set(a, &key_a, sizeof(key_a), &value_a, sizeof(value_a)) == 0 );
	rc = sp_rollback(a);

	t( rc == 0 );
	int value_b = 15;
	int key_b = 8;
	t( sp_set(b, &key_b, sizeof(key_b), &value_b, sizeof(value_b)) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );

	void *tx = sp_begin(db);
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key_a, sizeof(key_a), &v, &vsize) == 0 );
	t( sp_get(tx, &key_b, sizeof(key_b), &v, &vsize) == 0 );
	t( sp_rollback(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_rollback_get1(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value_a = 10;
	int key_a = 7;
	t( sp_set(a, &key_a, sizeof(key_a), &value_a, sizeof(value_a)) == 0 );
	rc = sp_rollback(a);

	t( rc == 0 );
	int value_b = 15;
	int key_b = 8;
	t( sp_set(b, &key_b, sizeof(key_b), &value_b, sizeof(value_b)) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );

	void *tx = sp_begin(db);
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key_a, sizeof(key_a), &v, &vsize) == 0 );
	t( sp_get(tx, &key_b, sizeof(key_b), &v, &vsize) == 0 );
	t( sp_rollback(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_p_set_rollback_get2(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );

	t( rc == 0 );
	int value_b = 15;
	int key_b = 8;
	t( sp_set(b, &key_b, sizeof(key_b), &value_b, sizeof(value_b)) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );

	int value_a = 10;
	int key_a = 7;
	t( sp_set(a, &key_a, sizeof(key_a), &value_a, sizeof(value_a)) == 0 );
	rc = sp_rollback(a);

	void *tx = sp_begin(db);
	void *v = NULL;
	int vsize = 0;
	t( sp_get(tx, &key_a, sizeof(key_a), &v, &vsize) == 0 );
	t( sp_get(tx, &key_b, sizeof(key_b), &v, &vsize) == 0 );
	t( sp_rollback(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.logdir", "log") == 0 );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );

	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit1(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );

	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit2(void)
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

	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_rollback_a0(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_rollback(a);
	t( rc == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_rollback_a1(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_rollback(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_rollback_b0(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_rollback_b1(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_rollback_ab0(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_rollback(a);
	t( rc == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_rollback_ab1(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_rollback(a);
	t( rc == 0 );
	rc = sp_rollback(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_a0(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(a);
	t( rc == 2 ); /* wait */
	rc = sp_commit(b);
	t( rc == 0 );
	rc = sp_commit(a);
	t( rc == 1 ); /* rlb */
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_a1(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(a);
	t( rc == 2 ); /* wait */
	rc = sp_commit(b);
	t( rc == 0 );
	rc = sp_commit(a);
	t( rc == 1 ); /* rlb */
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_b0(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_b1(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_a0(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_a1(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_b0(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(b);
	t( rc == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_b1(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	void *a = sp_begin(db);
	t( a != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(b);
	t( rc == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_n0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_ctl(env, "conf");
	t( sp_set(conf, "env.logdir", "log") == 0 );
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *db = sp_database(env);
	t( db != NULL );
	conf = sp_ctl(db, "conf");
	t( sp_set(conf, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	void *c = sp_begin(db);
	t( c != NULL );

	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(c, &key, sizeof(key), &value, sizeof(value)) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_n1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_ctl(env, "conf");
	t( sp_set(conf, "env.logdir", "log") == 0 );
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *db = sp_database(env);
	t( db != NULL );
	conf = sp_ctl(db, "conf");
	t( sp_set(conf, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *c = sp_begin(db);
	t( c != NULL );
	void *b = sp_begin(db);
	t( b != NULL );

	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(c, &key, sizeof(key), &value, sizeof(value)) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_n0(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_ctl(env, "conf");
	t( sp_set(conf, "env.logdir", "log") == 0 );
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *db = sp_database(env);
	t( db != NULL );
	conf = sp_ctl(db, "conf");
	t( sp_set(conf, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	void *c = sp_begin(db);
	t( c != NULL );

	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(c, &key, sizeof(key), &value, sizeof(value)) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(a);
	t( rc == 0 );
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_commit(b);
	t( rc == 0 );
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_n1(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_ctl(env, "conf");
	t( sp_set(conf, "env.logdir", "log") == 0 );
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *db = sp_database(env);
	t( db != NULL );
	conf = sp_ctl(db, "conf");
	t( sp_set(conf, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	void *c = sp_begin(db);
	t( c != NULL );

	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(c, &key, sizeof(key), &value, sizeof(value)) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(b);
	t( rc == 0 );
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(c);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_n2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_ctl(env, "conf");
	t( sp_set(conf, "env.logdir", "log") == 0 );
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *db = sp_database(env);
	t( db != NULL );
	conf = sp_ctl(db, "conf");
	t( sp_set(conf, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	void *c = sp_begin(db);
	t( c != NULL );

	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(c, &key, sizeof(key), &value, sizeof(value)) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(c);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_n3(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_ctl(env, "conf");
	t( sp_set(conf, "env.logdir", "log") == 0 );
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *db = sp_database(env);
	t( db != NULL );
	conf = sp_ctl(db, "conf");
	t( sp_set(conf, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	void *c = sp_begin(db);
	t( c != NULL );

	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(c, &key, sizeof(key), &value, sizeof(value)) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(c);
	t( rc == 0 );
	rc = sp_commit(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_commit_wait_rollback_n4(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_ctl(env, "conf");
	t( sp_set(conf, "env.logdir", "log") == 0 );
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *db = sp_database(env);
	t( db != NULL );
	conf = sp_ctl(db, "conf");
	t( sp_set(conf, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	void *c = sp_begin(db);
	t( c != NULL );

	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_set(c, &key, sizeof(key), &value, sizeof(value)) == 0 );

	rc = sp_commit(b);
	t( rc == 2 ); /* wait */
	rc = sp_commit(c);
	t( rc == 2 ); /* wait */
	rc = sp_rollback(c);
	t( rc == 0 );
	rc = sp_rollback(a);
	t( rc == 0 );
	rc = sp_commit(b);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_get0(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(a);
	t( rc == 0 );

	void *v = NULL;
	int vsize = 0;
	t( sp_get(b, &key, sizeof(key), &v, &vsize) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(b);
	t( rc == 1 ); /* rlb */

	void *tx = sp_begin(db);
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 10 );
	free(v);
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_get1(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *a = sp_begin(db);
	t( a != NULL );
	void *b = sp_begin(db);
	t( b != NULL );
	int value = 10;
	int key = 7;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_rollback(a);
	t( rc == 0 );

	value = 15;
	void *v = NULL;
	int vsize = 0;
	t( sp_get(b, &key, sizeof(key), &v, &vsize) == 0 );
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(b);
	t( rc == 0 );

	void *tx = sp_begin(db);
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 15 );
	free(v);
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_get2(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_ctl(env, "conf");
	t( sp_set(conf, "env.logdir", "log") == 0 );
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *db = sp_database(env);
	t( db != NULL );
	conf = sp_ctl(db, "conf");
	t( sp_set(conf, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *z = sp_begin(db);

	void *a = sp_begin(db);
	t( a != NULL );
	int key = 7;
	int value = 1;
	t( sp_set(a, &key, sizeof(key), &value, sizeof(value)) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	value = 2;
	t( sp_set(b, &key, sizeof(key), &value, sizeof(value)) == 0 );

	void *c = sp_begin(db);
	t( c != NULL );
	value = 3;
	t( sp_set(c, &key, sizeof(key), &value, sizeof(value)) == 0 );

	void *d = sp_begin(db);
	t( d != NULL );
	value = 4;
	t( sp_set(d, &key, sizeof(key), &value, sizeof(value)) == 0 );

	void *e = sp_begin(db);
	t( e != NULL );

	void *v = NULL;
	int vsize = 0;
	t( sp_get(a, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 1 );
	free(v);

	t( sp_get(b, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 2 );
	free(v);

	t( sp_get(c, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 3 );
	free(v);

	t( sp_get(d, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 4 );
	free(v);

	t( sp_get(e, &key, sizeof(key), &v, &vsize) == 0 );
	t( sp_get(z, &key, sizeof(key), &v, &vsize) == 0 );

	void *tx = sp_begin(db);
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 0 );
	rc = sp_rollback(tx);
	t( rc == 0 );

	t( sp_rollback(d) == 0 );
	t( sp_rollback(c) == 0 );
	t( sp_rollback(b) == 0 );
	t( sp_rollback(a) == 0 );
	t( sp_rollback(e) == 0 );
	t( sp_rollback(z) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_c_set_get3(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_ctl(env, "conf");
	t( sp_set(conf, "env.logdir", "log") == 0 );
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	int rc = sp_open(env);
	t( rc == 0 );
	
	void *db = sp_database(env);
	t( db != NULL );
	conf = sp_ctl(db, "conf");
	t( sp_set(conf, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	void *z = sp_begin(db);

	void *a = sp_begin(db);
	t( a != NULL );
	int key = 7;
	int value = 1;
	void *tx = sp_begin(db);
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_commit(tx) == 0 );

	void *b = sp_begin(db);
	t( b != NULL );
	value = 2;
	tx = sp_begin(db);
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_commit(tx) == 0 );

	void *c = sp_begin(db);
	t( c != NULL );
	value = 3;
	tx = sp_begin(db);
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_commit(tx) == 0 );

	void *d = sp_begin(db);
	t( d != NULL );
	value = 4;
	tx = sp_begin(db);
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	t( sp_commit(tx) == 0 );

	void *e = sp_begin(db);
	t( e != NULL );

	/* 0 */
	void *v = NULL;
	int vsize = 0;
	t( sp_get(b, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 1 );
	free(v);

	t( sp_get(c, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 2 );
	free(v);

	t( sp_get(d, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 3 );
	free(v);

	t( sp_get(e, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 4 );
	free(v);

	tx = sp_begin(db);
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 4 );
	free(v);
	t( sp_rollback(tx) == 0 );

	t( sp_get(a, &key, sizeof(key), &v, &vsize) == 0 );
	t( sp_get(z, &key, sizeof(key), &v, &vsize) == 0 );

	/* 1 */
	t( sp_rollback(b) == 0 );

	t( sp_get(c, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 2 );
	free(v);

	t( sp_get(d, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 3 );
	free(v);

	t( sp_get(e, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 4 );
	free(v);

	tx = sp_begin(db);
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 4 );
	free(v);
	t( sp_rollback(tx) == 0 );

	t( sp_get(a, &key, sizeof(key), &v, &vsize) == 0 );
	t( sp_get(z, &key, sizeof(key), &v, &vsize) == 0 );

	/* 2 */
	t( sp_rollback(c) == 0 );

	t( sp_get(d, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 3 );
	free(v);

	t( sp_get(e, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 4 );
	free(v);

	tx = sp_begin(db);
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 4 );
	free(v);
	t( sp_rollback(tx) == 0 );

	t( sp_get(a, &key, sizeof(key), &v, &vsize) == 0 );
	t( sp_get(z, &key, sizeof(key), &v, &vsize) == 0 );

	/* 3 */
	t( sp_rollback(d) == 0 );

	t( sp_get(e, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 4 );
	free(v);

	tx = sp_begin(db);
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 4 );
	free(v);
	t( sp_rollback(tx) == 0 );

	t( sp_get(a, &key, sizeof(key), &v, &vsize) == 0 );
	t( sp_get(z, &key, sizeof(key), &v, &vsize) == 0 );

	/* 4 */
	t( sp_rollback(e) == 0 );

	tx = sp_begin(db);
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 4 );
	free(v);
	t( sp_rollback(tx) == 0 );

	t( sp_get(a, &key, sizeof(key), &v, &vsize) == 0 );
	t( sp_get(z, &key, sizeof(key), &v, &vsize) == 0 );

	/* 6 */
	t( sp_rollback(a) == 0 );
	t( sp_rollback(z) == 0 );

	tx = sp_begin(db);
	t( sp_get(tx, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 4 );
	free(v);
	t( sp_rollback(tx) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_s_set(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	int key = 7;
	t( sp_set(db, &key, sizeof(key), &key, sizeof(key)) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_s_set_get(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	int key = 7;
	t( sp_set(db, &key, sizeof(key), &key, sizeof(key)) == 0 );

	void *value = NULL;
	int valuesize = 0;
	t( sp_get(db, &key, sizeof(key), &value, &valuesize) == 1 );
	t( *(int*)value == key );
	free(value);

	t( sp_destroy(env) == 0 );
}

static void
test_sc_set_wait(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	int key = 7;
	void *tx = sp_begin(db);
	t( tx != NULL );
	t( sp_set(tx, &key, sizeof(key), &key, sizeof(key)) == 0 );
	t( sp_set(db, &key, sizeof(key), &key, sizeof(key)) == 2 ); /* wait */
	rc = sp_commit(tx);
	t( rc == 0 );

	void *value = NULL;
	int valuesize = 0;
	t( sp_get(db, &key, sizeof(key), &value, &valuesize) == 1 );
	t( *(int*)value == key );
	free(value);
	t( sp_destroy(env) == 0 );
}

static void
test_sc_get(void)
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
	
	void *db = sp_database(env);
	t( db != NULL );
	c = sp_ctl(db, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_open(db) == 0 );

	int key = 7;
	int v = 7;
	t( sp_set(db, &key, sizeof(key), &key, sizeof(key)) == 0 );

	void *tx = sp_begin(db);
	t( tx != NULL );
	v = 8;
	t( sp_set(tx, &key, sizeof(key), &v, sizeof(v)) == 0 );

	void *value = NULL;
	int valuesize = 0;
	t( sp_get(db, &key, sizeof(key), &value, &valuesize) == 1 );
	t( *(int*)value == 7 );
	free(value);

	rc = sp_commit(tx);
	t( rc == 0 );

	value = NULL;
	valuesize = 0;
	t( sp_get(db, &key, sizeof(key), &value, &valuesize) == 1 );
	t( *(int*)value == 8 );
	free(value);

	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test_rollback );
	test( test_commit );
	test( test_set_commit );
	test( test_set_get_commit );
	test( test_set_commit_get0 );
	test( test_set_commit_get1 );
	test( test_set_rollback );
	test( test_set_rollback_get0 );
	test( test_set_rollback_get1 );

	test( test_set_set_commit );
	test( test_set_set_get_commit );
	test( test_set_set_commit_get );
	test( test_set_set_rollback_get );
	test( test_set_delete_get_commit );
	test( test_set_delete_get_commit_get );
	test( test_set_delete_set_commit_get );
	test( test_set_delete_commit_get_set );
	test( test_set_set_get_commit );
	test( test_p_set_commit );
	test( test_p_set_commit );
	test( test_p_set_get_commit );
	test( test_p_set_commit_get0 );
	test( test_p_set_commit_get1 );
	test( test_p_set_commit_get2 );
	test( test_p_set_rollback_get0 );
	test( test_p_set_rollback_get1 );
	test( test_p_set_rollback_get2 );
	test( test_c_set_commit0 );
	test( test_c_set_commit1 );
	test( test_c_set_commit2 );
	test( test_c_set_commit_rollback_a0 );
	test( test_c_set_commit_rollback_a1 );
	test( test_c_set_commit_rollback_b0 );
	test( test_c_set_commit_rollback_b1 );
	test( test_c_set_commit_rollback_ab0 );
	test( test_c_set_commit_rollback_ab1 );
	test( test_c_set_commit_wait_a0 );
	test( test_c_set_commit_wait_a1 );
	test( test_c_set_commit_wait_b0 );
	test( test_c_set_commit_wait_b1 );
	test( test_c_set_commit_wait_rollback_a0 );
	test( test_c_set_commit_wait_rollback_a1 );
	test( test_c_set_commit_wait_rollback_b0 );
	test( test_c_set_commit_wait_rollback_b1 );
	test( test_c_set_commit_wait_n0 );
	test( test_c_set_commit_wait_n1 );
	test( test_c_set_commit_wait_rollback_n0 );
	test( test_c_set_commit_wait_rollback_n1 );
	test( test_c_set_commit_wait_rollback_n2 );
	test( test_c_set_commit_wait_rollback_n3 );
	test( test_c_set_commit_wait_rollback_n4 );
	test( test_c_set_get0 );
	test( test_c_set_get1 );
	test( test_c_set_get2 );
	test( test_c_set_get3 );
	test( test_s_set );
	test( test_s_set_get );
	test( test_sc_set_wait );
	test( test_sc_get );
	return 0;
}
