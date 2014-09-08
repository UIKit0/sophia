#ifndef SOPHIA_H_
#define SOPHIA_H_

typedef void *(*spallocf)(void *ptr, size_t size, void *arg);
typedef int (*spcmpf)(char *a, size_t asz, char *b, size_t bsz, void *arg);



SP_API void *sp_env(void);
SP_API void *sp_open(void *env);
SP_API int sp_ctl(void*, spopt, ...);
SP_API int sp_destroy(void *ptr);
SP_API int sp_begin(void *db);
SP_API int sp_commit(void *db);
SP_API int sp_rollback(void *db);
SP_API int sp_set(void *db, const void *k, size_t ksize, const void *v, size_t vsize);
SP_API int sp_delete(void *db, const void *k, size_t ksize);
SP_API int sp_get(void *db, const void *k, size_t ksize, void **v, size_t *vsize);
SP_API void *sp_cursor(void *db, sporder, const void *k, size_t ksize);
SP_API int sp_fetch(void *cur);
SP_API const char *sp_key(void *cur);
SP_API size_t sp_keysize(void *cur);
SP_API const char *sp_value(void *cur);
SP_API size_t sp_valuesize(void *cur);
SP_API char *sp_error(void *ptr);
SP_API void sp_stat(void *ptr, spstat*);


#endif
