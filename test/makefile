
CC      ?= gcc
RM      ?= rm
CFLAGS  ?= -I. -I../env -I../store -I../log -I../mvcc -I../database -I../version -I../rt -Wall -pthread -O0 -g
LDFLAGS ?= -pthread ../env/libsophia.a

all: clean \
	tx \
	recover \
#	common \
	snapshot \
	backup \
	drop \
	tx_merge \
	seaveiter \
	scheme \
	ssmerge \
	merge \
	mergeiter \
	sliter \
	ssdbiter \
	rotate \
	cursor \
	cursor_merge \
	pageiter \
	smindex \
	smindexiter

ssmerge: ssmerge.o
	$(CC) ssmerge.o $(LDFLAGS) -o ssmerge
sliter: sliter.o
	$(CC) sliter.o $(LDFLAGS) -o sliter
snapshot: snapshot.o
	$(CC) snapshot.o $(LDFLAGS) -o snapshot
backup: backup.o
	$(CC) backup.o $(LDFLAGS) -o backup
drop: drop.o
	$(CC) drop.o $(LDFLAGS) -o drop
scheme: scheme.o
	$(CC) scheme.o $(LDFLAGS) -o scheme
recover: recover.o
	$(CC) recover.o $(LDFLAGS) -o recover
ssdbiter: ssdbiter.o
	$(CC) ssdbiter.o $(LDFLAGS) -o ssdbiter
rotate: rotate.o
	$(CC) rotate.o $(LDFLAGS) -o rotate
merge: merge.o
	$(CC) merge.o $(LDFLAGS) -o merge
cursor_merge: cursor_merge.o
	$(CC) cursor_merge.o $(LDFLAGS) -o cursor_merge
cursor: cursor.o
	$(CC) cursor.o $(LDFLAGS) -o cursor
tx_merge: tx_merge.o
	$(CC) tx_merge.o $(LDFLAGS) -o tx_merge
tx: tx.o
	$(CC) tx.o $(LDFLAGS) -o tx
mergeiter: mergeiter.o
	$(CC) mergeiter.o $(LDFLAGS) -o mergeiter
seaveiter: seaveiter.o
	$(CC) seaveiter.o $(LDFLAGS) -o seaveiter
common: common.o
	$(CC) common.o $(LDFLAGS) -o common
pageiter: pageiter.o
	$(CC) pageiter.o $(LDFLAGS) -o pageiter
smindex: smindex.o
	$(CC) smindex.o $(LDFLAGS) -o smindex
smindexiter: smindexiter.o
	$(CC) smindexiter.o $(LDFLAGS) -o smindexiter
.c.o:
	$(CC) $(CFLAGS) -c $<
clean:
	$(RM) -f ssmerge.o ssmerge
	$(RM) -f common.o common
	$(RM) -f sliter.o sliter
	$(RM) -f scheme.o scheme
	$(RM) -f snapshot.o snapshot
	$(RM) -f backup.o backup
	$(RM) -f drop.o drop
	$(RM) -f recover.o recover
	$(RM) -f ssdbiter.o ssdbiter
	$(RM) -f rotate.o rotate
	$(RM) -f merge.o merge
	$(RM) -f cursor_merge.o cursor_merge
	$(RM) -f cursor.o cursor
	$(RM) -f tx_merge.o tx_merge
	$(RM) -f tx.o tx
	$(RM) -f mergeiter.o mergeiter
	$(RM) -f seaveiter.o seaveiter
	$(RM) -f pageiter.o pageiter
	$(RM) -f smindex.o smindex
	$(RM) -f smindexiter.o smindexiter
#$(RM) -rf log test
test:
	$(VALGRIND) ./ssmerge
	$(VALGRIND) ./sliter
	$(VALGRIND) ./scheme
	$(VALGRIND) ./backup
	$(VALGRIND) ./drop
	$(VALGRIND) ./snapshot
	$(VALGRIND) ./recover
	$(VALGRIND) ./ssdbiter
	$(VALGRIND) ./rotate
	$(VALGRIND) ./merge
	$(VALGRIND) ./cursor_merge
	$(VALGRIND) ./cursor
	$(VALGRIND) ./tx_merge
	$(VALGRIND) ./tx
	$(VALGRIND) ./mergeiter
	$(VALGRIND) ./seaveiter
	$(VALGRIND) ./pageiter
	$(VALGRIND) ./smindex
	$(VALGRIND) ./smindexiter
