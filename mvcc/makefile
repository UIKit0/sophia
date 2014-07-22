LIBSM_O = sm_v.o sm_index.o sm_indexiter.o sm.o sm_iter.o
LIBSM_OBJECTS = $(addprefix mvcc/, $(LIBSM_O))
OBJECTS = $(LIBSM_O)
ifndef buildworld
  include ../build/config
  include ../build/lib
endif
