
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <time.h>
#include <assert.h>
#include <sys/time.h>
#include <string.h>
#include <sophia.h>
#include "test.h"

#include <sys/time.h>
#include <time.h>

unsigned long long now(void)
{
	unsigned long long tm;
	struct timeval tv;
	gettimeofday(&tv, NULL);
	tm = ((long)tv.tv_sec) * 1000;
	tm += tv.tv_usec / 1000;
	return tm;
}

static inline void
print_current(int i) {
	if (i > 0 && (i % 100000) == 0)
		printf("%.1fM\n", i / 1000000.0);
}

unsigned long long begin = 0;
int n = 0;

void qos(int limit)
{
	if (n == limit) {
		unsigned long long c = now();
		unsigned long long diff = c - begin;
		float rps = n / (diff / 1000.0);

		if (rps > limit) {
			double t = ((rps - limit) * ( (double)diff / (double)n ));
			/*printf("sleep: %f\n", t * 1000.0);*/
			usleep(t * 1000.0);
		}
		begin = now();
		n = 0;
	}

	n++;
}

int
main(int argc, char *argv[])
{
	if (argc == 1)
		return 0;
	int set = 0;
	int get = 0;
	if (strcmp(argv[1], "set") == 0)
		set = 1;
	else
	if (strcmp(argv[1], "get") == 0)
		get = 1;
	else
	if (strcmp(argv[1], "setget") == 0) {
		set = 1;
		get = 1;
	}

	if (set) {
		rmrf("./test");
		rmrf("./log");
	}

	void *env = sp_env();
	t( env != NULL );

	void *c = sp_use(env, "conf");
	t( c != NULL );
	t( sp_set(c, "env.dir", "test") == 0 );
	t( sp_set(c, "env.threads", 4) == 0 );
	t( sp_set(c, "env.scheduler", 1) == 0 );
	t( sp_set(c, "env.db_gc", 1) == 0 );

	int rc = sp_open(env);
	t( rc == 0 );

	if (set) {
		void *scheme = sp_use(env, "scheme");
		t( scheme != NULL );
		void *stx = sp_begin(scheme);
		t( sp_set(stx, "test") == 0);
		t( sp_set(stx, "test.cmp", "u32") == 0 );
		t( sp_commit(stx) == 0 );
		t( sp_destroy(scheme) == 0 );
	}

	void *db = sp_use(env, "test");
	assert( db != NULL );

	char value[100];
	memset(value, 0, sizeof(value));

	srand(8235121);

	unsigned long long diff;
	float rps;
	unsigned long long start = now();
	int n = 10000000;
	int i, k;

	if (set) {
		for (i = 0; i < n; i++) {
			/*qos(80000);*/
			k = rand();
			t( sp_set(db, &k, sizeof(k), value, sizeof(value)) == 0 );
			print_current(i);
		}

		diff = now() - start;
		rps = n / (diff / 1000.0);
		printf("%d SET rps\n", (int)rps);

	}

	if (get) {

		srand(8235121);
		start = now();

		for (i = 0; i < n; i++) {
			k = rand();
			void *value = NULL;
			int valuesize = 0;
			t( sp_get(db, &k, sizeof(k), &value, &valuesize) == 1 );
			free(value);

			print_current(i);
		}

		diff = now() - start;
		rps = n / (diff / 1000.0);
		printf("%d GET rps\n", (int)rps);
	}

	t( sp_destroy(env) == 0 );
	return 0;
}
