LIBSV_O = sv_local.o sv_mergeiter.o sv_seaveiter.o
LIBSV_OBJECTS = $(addprefix version/, $(LIBSV_O))
OBJECTS = $(LIBSV_O)
ifndef buildworld
  include ../build/config
  include ../build/lib
endif
