
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsl.h>
#include <libss.h>
#include <libsm.h>
#include <libsd.h>
#include <libse.h>
#include <sophia.h>

SP_API void*
sp_env(void)
{
	return se_new();
}

SP_API void*
sp_database(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->database == NULL))
		return NULL;
	va_list args;
	va_start(args, o);
	void *h = oif->database(o, args);
	va_end(args);
	return h;
}

SP_API void*
sp_ctl(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->ctl == NULL))
		return NULL;
	va_list args;
	va_start(args, o);
	void *h = oif->ctl(o, args);
	va_end(args);
	return h;
}

SP_API int
sp_open(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->open == NULL))
		return -1;
	va_list args;
	va_start(args, o);
	int rc = oif->open(o, args);
	va_end(args);
	return rc;
}

SP_API int
sp_destroy(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->destroy == NULL))
		return -1;
	return oif->destroy(o);
}

SP_API int sp_drop(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->drop == NULL))
		return -1;
	va_list args;
	int rc = oif->drop(o, args);
	va_end(args);
	return rc;
}

SP_API int
sp_set(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->set == NULL))
		return -1;
	va_list args;
	va_start(args, o);
	int rc = oif->set(o, args);
	va_end(args);
	return rc;
}

SP_API int
sp_get(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->get == NULL))
		return -1;
	va_list args;
	va_start(args, o);
	int rc = oif->get(o, args);
	va_end(args);
	return rc;
}

SP_API int
sp_delete(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->del == NULL))
		return -1;
	va_list args;
	va_start(args, o);
	int rc = oif->del(o, args);
	va_end(args);
	return rc;
}

SP_API void*
sp_begin(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->begin == NULL))
		return NULL;
	return oif->begin(o);
}

SP_API int
sp_commit(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->commit == NULL))
		return -1;
	va_list args;
	va_start(args, o);
	int rc = oif->commit(o, args);
	va_end(args);
	return rc;
}

SP_API int
sp_rollback(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->rollback == NULL))
		return -1;
	return oif->rollback(o);
}

SP_API void*
sp_cursor(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->cursor == NULL))
		return NULL;
	va_list args;
	va_start(args, o);
	char *order = va_arg(args, char*);
	srorder cmp;
	if (strcmp(order, ">") == 0)
		cmp = SR_GT;
	else
	if (strcmp(order, ">=") == 0)
		cmp = SR_GTE;
	else
	if (strcmp(order, "<") == 0)
		cmp = SR_LT;
	else
	if (strcmp(order, "<=") == 0)
		cmp = SR_LTE;
	else
		return NULL;
	void *key = va_arg(args, void*);
	int keysize = va_arg(args, int);
	void *cursor = oif->cursor(o, cmp, key, keysize);
	va_end(args);
	return cursor;
}

SP_API int
sp_fetch(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->fetch == NULL))
		return -1;
	return oif->fetch(o);
}

SP_API void*
sp_key(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->key == NULL))
		return NULL;
	return oif->key(o);
}

SP_API size_t
sp_keysize(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->keysize == NULL))
		return 0;
	return oif->keysize(o);
}

SP_API void*
sp_value(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->value == NULL))
		return NULL;
	return oif->value(o);
}

SP_API size_t
sp_valuesize(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->valuesize == NULL))
		return 0; 
	return oif->valuesize(o);
}

SP_API void*
sp_backup(void *o, ...)
{
	seobjif *oif = ((seobj*)o)->oif;
	if (srunlikely(oif->backup == NULL))
		return NULL;
	return oif->backup(o);
}

SP_API char*
sp_error(void *o, ...)
{
	(void)o;
	return "";
}
