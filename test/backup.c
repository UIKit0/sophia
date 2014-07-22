
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

static int
copyfile(char *path, char *dir)
{
	char *name = strrchr(path, '/');
	if (name == NULL)
		name = path;
	char dest[PATH_MAX];
	snprintf(dest, sizeof(dest), "%s/%s", dir, name);
	int fromfd = open(path, O_RDONLY);
	if (fromfd == -1)
		return -1;
	int destfd = open(dest, O_RDWR|O_CREAT, 0644);
	if (destfd == -1) {
		close(fromfd);
		return -1;
	}
	int rc = 0;
	for (;;) {
		char buf[4096];
		int rc = read(fromfd, buf, sizeof(buf));
		if (rc <= 0)
			break;
		rc = write(destfd, buf, rc);
		if (rc == -1)
			break;
	}
	close(fromfd);
	close(destfd);
	return rc;
}

static void
test_backup_empty(void)
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
	
	void *backup = sp_backup(env);
	t( backup != NULL );
	t( sp_fetch(backup) == 1 );
	t( sp_key(backup) != NULL );
	t( sp_value(backup) == NULL );
	t( sp_fetch(backup) == 1 );
	t( sp_key(backup) != NULL );
	t( sp_fetch(backup) == 0 );
	t( sp_value(backup) == NULL );
	t( sp_key(backup) == NULL );
	t( sp_destroy(backup) == 0 );

	t( sp_destroy(env) == 0 );
}

static void
test_backup_and_recover(void)
{
	rmrf("./test");
	rmrf("./test_bkup");
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

	char *dest = "./test_bkup";
	t( mkdir(dest, 0755) == 0);
	
	void *backup = sp_backup(env);
	t( backup != NULL );
	while (sp_fetch(backup)) {
		char *path = sp_key(backup);
		t( copyfile(path, dest) == 0 );
	}
	t( sp_destroy(backup) == 0 );
	t( sp_destroy(env) == 0 );

	/* start from backup copy */
	env = sp_env();
	t( env != NULL );
	c = sp_use(env, "conf");
	t( sp_set(c, "env.logdir", dest) == 0 );
	t( sp_set(c, "env.dir", dest) == 0 );
	t( sp_set(c, "env.threads", 0) == 0 );
	t( sp_set(c, "env.scheduler", 0) == 0 );
	rc = sp_open(env);
	t( rc == 0 );
	db = sp_use(env, "test");
	t( db != NULL );
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

	rmrf("./test_bkup");
}

int
main(int argc, char *argv[])
{
	test( test_backup_empty );
	test( test_backup_and_recover );
	return 0;
}
