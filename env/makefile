LIBSE_O = se.o \
          se_db.o \
          se_tx.o \
          se_cursor.o \
          se_scheme.o \
          se_recover.o \
          se_backup.o \
          se_task.o \
          se_process.o \
          se_schedule.o \
          se_cmd.o \
          sophia.o
LIBSE_OBJECTS = $(addprefix env/, $(LIBSE_O))
OBJECTS = $(LIBSE_O)
ifndef buildworld
  include ../build/config
  include ../build/lib
endif
