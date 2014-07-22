
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
test_empty(void)
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
	t( sp_set(ctl, "merge") == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_merge(void)
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
	int key = 1;
	int value = 1;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	key = 2;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	key = 3;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );
	t( sp_destroy(env) == 0 );
}

static void
test_merge_rewrite(void)
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
	int key = 1;
	int value = 1;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	key = 2;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	key = 3;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 1 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 2 );
	t( *(int*)sp_value(c) == 1 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 3 );
	t( *(int*)sp_value(c) == 1 );
	sp_destroy(c);

	tx = sp_begin(db);
	t( tx != NULL );
	key = 1;
	value = 2;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	key = 2;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	key = 3;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );

	c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 2 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 3 );
	t( *(int*)sp_value(c) == 2 );
	sp_destroy(c);

	t( sp_destroy(env) == 0 );
}

static void
test_merge_rewrite_tx(void)
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
	int key = 1;
	int value = 1;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	key = 2;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	key = 3;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	void *snap = sp_begin(db);
	t( snap != NULL );

	t( sp_set(sp_ctl(db), "merge") == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 1 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 2 );
	t( *(int*)sp_value(c) == 1 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 3 );
	t( *(int*)sp_value(c) == 1 );
	sp_destroy(c);

	tx = sp_begin(db);
	t( tx != NULL );
	key = 1;
	value = 2;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	key = 2;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	key = 3;
	t( sp_set(tx, &key, sizeof(key), &value, sizeof(value)) == 0 );
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );

	c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 1 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 2 );
	t( *(int*)sp_value(c) == 2 );
	t( sp_fetch(c) == 1 );
	t( *(int*)sp_key(c) == 3 );
	t( *(int*)sp_value(c) == 2 );
	sp_destroy(c);

	void *v = NULL;
	int vsize = 0;
	key = 1;
	t( sp_get(snap, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 1 );
	free(v);
	key = 2;
	t( sp_get(snap, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 1 );
	free(v);
	key = 3;
	t( sp_get(snap, &key, sizeof(key), &v, &vsize) == 1 );
	t( *(int*)v == 1 );
	free(v);
	t( sp_rollback(snap) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_merge_page(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	t( sp_set(conf, "env.page_size", 16) == 0 );
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
	int value = 1;
	while (i < 68) {
		t( sp_set(tx, &i, sizeof(i), &value, sizeof(value)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	i = 0;
	while (sp_fetch(c)) {
		t( *(int*)sp_key(c) == i );
		t( *(int*)sp_value(c) == 1 );
		i++;
	}
	t(i == 68 );
	sp_destroy(c);
	t( sp_destroy(env) == 0 );
}

static void
test_merge_node(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *conf = sp_use(env, "conf");
	t( sp_set(conf, "env.dir", "test") == 0 );
	t( sp_set(conf, "env.threads", 0) == 0 );
	t( sp_set(conf, "env.scheduler", 0) == 0 );
	t( sp_set(conf, "env.node_size", 1) == 0 );
	t( sp_set(conf, "env.page_size", 6) == 0 );
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
	int value = 1;
	while (i < 68) {
		t( sp_set(tx, &i, sizeof(i), &value, sizeof(value)) == 0 );
		i++;
	}
	rc = sp_commit(tx);
	t( rc == 0 );

	t( sp_set(sp_ctl(db), "merge") == 0 );

	void *c = sp_cursor(db, ">=", NULL, 0);
	t( c != NULL );
	i = 0;
	while (sp_fetch(c)) {
		t( *(int*)sp_key(c) == i );
		t( *(int*)sp_value(c) == 1 );
		i++;
	}
	t(i == 68 );
	sp_destroy(c);

	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test_empty );
	test( test_merge );
	test( test_merge_rewrite );
	test( test_merge_rewrite_tx );
	test( test_merge_page );
	test( test_merge_node );
	return 0;
}
