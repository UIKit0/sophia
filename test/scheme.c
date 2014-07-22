
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
test_scheme_set_constraint_scheme(void)
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

	void *tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_set(tx, "scheme") == -1 ); /* alter denied */
	t ( sp_commit(tx) == 0 );

	sp_destroy(scheme);
	t( sp_destroy(env) == 0 );
}

static void
test_scheme_set_constraint_conf(void)
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

	void *tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_set(tx, "conf") == -1 ); /* alter denied */
	t ( sp_commit(tx) == 0 );

	sp_destroy(scheme);
	t( sp_destroy(env) == 0 );
}

static void
test_scheme_set_constraint_cmp(void)
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

	void *tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_set(tx, "cmp") == -1 ); /* alter denied */
	t ( sp_commit(tx) == 0 );

	sp_destroy(scheme);
	t( sp_destroy(env) == 0 );
}

static void
test_scheme_set_commit0(void)
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

	void *tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_set(tx, "test") == 0 );
	t( sp_commit(tx) == 0 );

	sp_destroy(scheme);

	void *test = sp_use(env, "test");
	t( test != NULL );
	sp_destroy(test);

	t( sp_destroy(env) == 0 );
}

static void
test_scheme_set_commit1(void)
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

	void *tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_set(tx, "test0") == 0 );
	t( sp_set(tx, "test1") == 0 );
	t( sp_commit(tx) == 0 );
	sp_destroy(scheme);

	void *test0 = sp_use(env, "test0");
	t( test0 != NULL );
	void *test1 = sp_use(env, "test1");
	t( test1 != NULL );
	sp_destroy(test0);
	sp_destroy(test1);

	t( sp_destroy(env) == 0 );
}

static void
test_scheme_set_rollback(void)
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

	void *tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_set(tx, "test") == 0 );
	t( sp_rollback(tx) == 0 );
	sp_destroy(scheme);
	void *test = sp_use(env, "test");
	t( test == NULL );
	t( sp_destroy(env) == 0 );
}

static void
test_scheme_set_update(void)
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

	void *tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_set(tx, "test") == 0 );
	t( sp_set(tx, "test.cmp", "u32") == 0 );
	t( sp_commit(tx) == 0 );
	sp_destroy(scheme);

	void *test = sp_use(env, "test");
	t( test != NULL );
	sp_destroy(test);

	t( sp_destroy(env) == 0 );
}

static void
test_scheme_use(void)
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
	sp_destroy(scheme);
	t( sp_destroy(env) == 0 );
}

static void
test_scheme_use_db(void)
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

	void *tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_set(tx, "test") == 0 );
	t( sp_commit(tx) == 0 );

	sp_destroy(scheme);

	void *test = sp_use(env, "test");
	t( test != NULL );
	sp_destroy(test);

	t( sp_destroy(env) == 0 );
}

static void
test_scheme_set_c_commit0(void)
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

	void *t0 = sp_begin(scheme);
	t( t0 != NULL );
	void *t1 = sp_begin(scheme);
	t( t1 != NULL );

	t( t0 != NULL );
	t( sp_set(t0, "test") == 0 );

	t( t1 != NULL );
	t( sp_set(t1, "test") == 0 );

	t( sp_commit(t0) == 0 );
	t( sp_commit(t1) == 1 ); /* rlb */

	sp_destroy(scheme);

	void *test = sp_use(env, "test");
	t( test != NULL );
	sp_destroy(test);

	t( sp_destroy(env) == 0 );
}

static void
test_scheme_set_c_commit1(void)
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

	void *t0 = sp_begin(scheme);
	t( t0 != NULL );

	void *t1 = sp_begin(scheme);
	t( t1 != NULL );

	t( sp_set(t1, "test") == 0 );
	t( sp_set(t1, "test.cmp", "string") == 0 );

	t( sp_set(t0, "test") == 0 );
	t( sp_set(t0, "test.cmp", "u32") == 0 );

	t( sp_commit(t0) == 2 ); /* wait */
	t( sp_commit(t1) == 0 );
	t( sp_commit(t0) == 1 ); /* rlb */

	sp_destroy(scheme);

	void *test = sp_use(env, "test");
	t( test != NULL );
	sp_destroy(test);

	t( sp_destroy(env) == 0 );
}

static void
test_scheme_set_c_commit2(void)
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

	void *t0 = sp_begin(scheme);
	t( t0 != NULL );

	void *t1 = sp_begin(scheme);
	t( t1 != NULL );

	t( sp_set(t1, "test") == 0 );
	t( sp_set(t1, "test.cmp", "string") == 0 );

	t( sp_set(t0, "test") == 0 );
	t( sp_set(t0, "test.cmp", "u32") == 0 );

	t( sp_rollback(t1) == 0 );
	t( sp_commit(t0) == 0 );

	sp_destroy(scheme);

	void *test = sp_use(env, "test");
	t( test != NULL );
	sp_destroy(test);

	t( sp_destroy(env) == 0 );
}

static void
test_scheme_set_commit_set(void)
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

	void *tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_set(tx, "test") == 0 );
	t ( sp_commit(tx) == 0 );

	void *test = sp_use(env, "test");
	t( test != NULL );
	sp_destroy(test);

	tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_set(tx, "test") == -1 );
	t( sp_commit(tx) == 0 );
	sp_destroy(scheme);

	t( sp_destroy(env) == 0 );
}

static void
test_scheme_set_delete_commit(void)
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

	void *tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_set(tx, "test") == 0 );
	t( sp_delete(tx, "test") == 0 );
	t ( sp_commit(tx) == 0 );

	void *test = sp_use(env, "test");
	t( test == NULL );

	sp_destroy(scheme);
	t( sp_destroy(env) == 0 );
}

static void
test_scheme_set_delete_rollback(void)
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

	void *tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_set(tx, "test") == 0 );
	t( sp_delete(tx, "test") == 0 );
	t ( sp_rollback(tx) == 0 );

	void *test = sp_use(env, "test");
	t( test == NULL );

	sp_destroy(scheme);
	t( sp_destroy(env) == 0 );
}

static void
test_scheme_set_commit_delete_commit(void)
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

	void *tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_set(tx, "test") == 0 );
	t ( sp_commit(tx) == 0 );

	void *test = sp_use(env, "test");
	t( test != NULL );
	sp_destroy(test);

	tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_delete(tx, "test") == 0 );
	t ( sp_commit(tx) == 0 );

	test = sp_use(env, "test");
	t( test == NULL );

	// merge

	sp_destroy(scheme);
	t( sp_destroy(env) == 0 );
}

static void
test_scheme_delete_commit(void)
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

	void *tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_delete(tx, "test") == -1 );
	t ( sp_commit(tx) == 0 );

	void *test = sp_use(env, "test");
	t( test == NULL );

	sp_destroy(scheme);
	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test_scheme_set_constraint_scheme );
	test( test_scheme_set_constraint_conf );
	test( test_scheme_set_constraint_cmp );

	test( test_scheme_set_commit0 );
	test( test_scheme_set_commit1 );
	test( test_scheme_set_rollback );
	test( test_scheme_set_update );
	test( test_scheme_use );
	test( test_scheme_use_db );
	test( test_scheme_set_c_commit0 );
	test( test_scheme_set_c_commit1 );
	test( test_scheme_set_c_commit2 );
	test( test_scheme_set_commit_set );
	test( test_scheme_set_delete_commit );
	test( test_scheme_set_delete_rollback );
	test( test_scheme_set_commit_delete_commit );
	test( test_scheme_delete_commit );
	return 0;
}
