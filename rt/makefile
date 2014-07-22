LIBSR_O = sr_crc.o \
          sr_cmp.o \
          sr_bufiter.o \
          sr_thread.o \
          sr_conf.o \
          sr_scheme.o \
          sr_file.o \
          sr_map.o \
          sr_store.o
LIBSR_OBJECTS = $(addprefix rt/, $(LIBSR_O))
OBJECTS = $(LIBSR_O)
ifndef buildworld
  include ../build/config
  include ../build/lib
endif
