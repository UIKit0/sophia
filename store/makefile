LIBSS_O = ss_v.o \
          ss_pagebuild.o \
          ss_pageiter.o \
          ss_index.o \
          ss_indexiter.o \
          ss_track.o \
          ss_db.o \
          ss_dbiter.o \
          ss.o \
          ss_merge.o \
          ss_gc.o \
          ss_copy.o
LIBSS_OBJECTS = $(addprefix store/, $(LIBSS_O))
OBJECTS = $(LIBSS_O)
ifndef buildworld
  include ../build/config
  include ../build/lib
endif
