LIBSD_O = sd_pair.o \
          sd_node.o \
          sd_nodeindex.o \
          sd_index.o \
          sd_indexiter.o \
          sd.o \
          sd_query.o \
          sd_commit.o \
          sd_split.o \
          sd_merge.o \
          sd_gc.o
LIBSD_OBJECTS = $(addprefix database/, $(LIBSD_O))
OBJECTS = $(LIBSD_O)
ifndef buildworld
  include ../build/config
  include ../build/lib
endif
