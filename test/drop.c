
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
test_drop_schedule(void)
{
	rmrf("./test");
	rmrf("./log");
	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
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

	int key = 7;
	t( sp_set(test, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( sp_set(test, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 9;
	t( sp_set(test, &key, sizeof(key), &key, sizeof(key)) == 0 );
	sp_destroy(test);

	t( sp_drop(test) == 0 );

	/* mark for drop/merge, do merge */
	t( sp_set(sp_ctl(env, "ctl"), "schedule") == 0 );
	/* actual drop */
	t( sp_set(sp_ctl(env, "ctl"), "schedule") == 0 );

	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test_drop_schedule );
	return 0;
}
