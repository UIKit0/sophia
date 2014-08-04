#ifndef LIBSR_H_
#define LIBSR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#define _GNU_SOURCE 1

#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <limits.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>

#include <sr_macro.h>
#include <sr_spinlock.h>
#include <sr_list.h>
#include <sr_a.h>
#include <sr_e.h>
#include <sr_flags.h>
#include <sr_gc.h>
#include <sr_seq.h>
#include <sr_order.h>
#include <sr_cmp.h>
#include <sr_conf.h>
#include <sr_buf.h>
#include <sr_qsort.h>
#include <sr_scheme.h>
#include <sr_c.h>
#include <sr_iter.h>
#include <sr_bufiter.h>
#include <sr_mutex.h>
#include <sr_thread.h>
#include <sr_crc.h>
#include <sr_path.h>
#include <sr_iov.h>
#include <sr_file.h>
#include <sr_map.h>
#include <sr_store.h>

#endif
