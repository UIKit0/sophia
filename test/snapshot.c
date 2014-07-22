
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
test_snapshot_empty(void)
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
	t( sp_set(sp_ctl(env), "snapshot") == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_snapshot_empty_recover(void)
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
	t( sp_set(sp_ctl(env), "snapshot") == 0 );
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
	t( sp_destroy(env) == 0 );
}

static void
test_snapshot0(void)
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

	void *db = sp_use(env, "test");
	t( db != NULL );
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

	t( sp_set(sp_ctl(env), "snapshot") == 0 );

	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_snapshot0_recover(void)
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

	void *db = sp_use(env, "test");
	t( db != NULL );
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

	t( sp_set(sp_ctl(env), "snapshot") == 0 );

	t( sp_destroy(db) == 0 );
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
	db = sp_use(env, "test");
	c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 7 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 8 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 9 );
	t( sp_fetch(c) == 0 );
	t( sp_destroy(c) == 0 );
	t( sp_destroy(db) == 0 );
	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test_snapshot_empty );
	test( test_snapshot_empty_recover );
	test( test_snapshot0 );
	test( test_snapshot0_recover );
	return 0;
}
