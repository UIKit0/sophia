
# sophia makefile.
#

CC             ?= gcc
AR              = ar
RM              = rm
LN              = ln
STRIP           = strip
VERMAJOR        = 1
VERMINOR        = 1
TARGET_STATIC   = libsophia.a
TARGET_DSOLIB   = libsophia.so.$(VERMAJOR).$(VERMINOR)
TARGET_DSO      = libsophia.so
TARGET_CFLAGS   = -I. -std=c99 -pedantic -Wextra -Wall -fPIC -fvisibility=hidden -O2 -DNDEBUG
#  -pthread
TARGET_LDFLAGS  = -dylib -lc
CFLAGS         := $(TARGET_CFLAGS) $(CFLAGS) -fno-stack-protector
LDFLAGS        := $(TARGET_LDFLAGS) -dylib  $(LDFLAGS)

# -soname $(TARGET_DSO).$(VERMAJOR)
# -soname $(TARGET_DSO).$(VERMAJOR)
# is -install_name

OBJS = file.o \
	crc.o \
	e.o \
	i.o \
	cat.o \
	rep.o \
	util.o \
	sp.o \
	recover.o \
	merge.o \
	gc.o \
	cursor.o

ALL: $(TARGET_STATIC) $(TARGET_DSO)

$(TARGET_STATIC): $(OBJS)
	$(AR) crus $(TARGET_STATIC) $(OBJS)

$(TARGET_DSO): $(OBJS)
	$(LD) $(OBJS) $(LDFLAGS) -o $(TARGET_DSOLIB)
	$(LN) -s $(TARGET_DSOLIB) $(TARGET_DSO).$(VERMAJOR)
	$(LN) -s $(TARGET_DSOLIB) $(TARGET_DSO)
	#$(STRIP) --strip-unneeded $(TARGET_DSO)

.c.o:
	$(CC) $(CFLAGS) -c $<

clean:
	$(RM) -f $(OBJS) $(TARGET_STATIC)
	$(RM) -f $(TARGET_DSOLIB) $(TARGET_DSO).$(VERMAJOR) $(TARGET_DSO)
