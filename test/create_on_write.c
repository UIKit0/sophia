
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
test_create_on_write(void)
{
	rmrf("./test");
	rmrf("./log");

	void *env = sp_env();
	t( env != NULL );
	void *c = sp_ctl(env, "conf");
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	t( sp_set(c, "env.create_on_write", 1) == 0 );
	sp_destroy(c);
	int rc = sp_open(env);
	t( rc == 0 );
	t( exists("test", "0000.db") == 0 );

	void *test = sp_database(env);
	t( test != NULL );
	c = sp_ctl(test, "conf");
	t( sp_set(c, "db.cmp", sr_cmpu32, NULL) == 0 );
	t( sp_set(c, "db.id", 64) == 0 );
	t( sp_open(test) == 0 );

	int key = 7;
	t( sp_set(test, &key, sizeof(key), &key, sizeof(key)) == 0 );
	key = 8;
	t( exists("test", "0000.db") == 0 );
	
	t( sp_set(sp_ctl(test, "ctl"), "merge") == 0 );
	t( exists("test", "0000.db") == 1 );

	t( sp_destroy(env) == 0 );
}

int
main(int argc, char *argv[])
{
	test( test_create_on_write );
	return 0;
}
