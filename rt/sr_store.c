
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

static inline ssize_t sr_storeidof(char *s)
{
	size_t v = 0;
	while (*s && *s != '.') {
		if (srunlikely(!isdigit(*s)))
			return -1;
		v = (v * 10) + *s - '0';
		s++;
	}
	return v;
}

static inline srstoreid*
sr_storematch(srbuf *list, uint64_t id)
{
	if (srunlikely(sr_bufused(list) == 0))
		return NULL;
	srstoreid *n = (srstoreid*)list->s;
	while ((char*)n < list->p) {
		if (n->id == id)
			return n;
		n++;
	}
	return NULL;
}

static inline srstoretype*
sr_storetypeof(srstoretype *types, char *ext)
{
	srstoretype *p = &types[0];
	int n = 0;
	while (p[n].ext != NULL) {
		if (strcmp(p[n].ext, ext) == 0)
			return &p[n];
		n++;
	}
	return NULL;
}

static int
sr_storecmp(const void *p1, const void *p2)
{
	srstoreid *a = (srstoreid*)p1;
	srstoreid *b = (srstoreid*)p2;
	assert(a->id != b->id);
	return (a->id > b->id)? 1: -1;
}

int sr_storeread(srbuf *list, sra *a, srstoretype *types, char *dir)
{
	DIR *d = opendir(dir);
	if (srunlikely(d == NULL))
		return -1;

	struct dirent *de;
	while ((de = readdir(d))) {
		if (srunlikely(de->d_name[0] == '.'))
			continue;
		ssize_t id = sr_storeidof(de->d_name);
		if (srunlikely(id == -1))
			goto error;
		char *ext = strstr(de->d_name, ".");
		if (srunlikely(ext == NULL))
			goto error;
		ext++;
		srstoretype *type = sr_storetypeof(types, ext);
		if (srunlikely(type == NULL))
			continue;
		srstoreid *n = sr_storematch(list, id);
		if (n) {
			n->mask |= type->mask;
			type->count++;
			continue;
		}
		int rc = sr_bufensure(list, a, sizeof(srstoreid));
		if (srunlikely(rc == -1))
			goto error;
		n = (srstoreid*)list->p;
		sr_bufadvance(list, sizeof(srstoreid));
		n->id  = id;
		n->mask = type->mask;
		type->count++;
	}
	closedir(d);

	if (srunlikely(sr_bufused(list) == 0))
		return 0;

	int n = sr_bufused(list) / sizeof(srstoreid);
	qsort(list->s, n, sizeof(srstoreid), sr_storecmp);
	return n;

error:
	closedir(d);
	return -1;
}
