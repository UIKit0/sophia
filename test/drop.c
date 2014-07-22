
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
test_drop_schedule(void)
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
	int key = 7;
	t( sp_set(test, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(test, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(test, &key, sizeof(key), &key, sizeof(key)) == 0 );
	sp_destroy(test);

	tx = sp_begin(scheme);
	t( tx != NULL );
	t( sp_delete(tx, "test") == 0 );
	t( sp_commit(tx) == 0 );

	/* mark for drop/merge, do merge */
	t( sp_set(sp_ctl(env), "schedule") == 0 );
	/* actual drop */
	t( sp_set(sp_ctl(env), "schedule") == 0 );

	test = sp_use(env, "test");
	t( test == NULL );

	sp_destroy(scheme);
	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test_drop_schedule );
	return 0;
}
