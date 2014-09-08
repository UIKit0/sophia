package main

type spopt int

const (
	SPDIR     spopt = iota /* uint32_t, char* */
	SPALLOC                /* spallocf, void* */
	SPCMP                  /* spcmpf, void* */
	SPPAGE                 /* uint32_t */
	SPGC                   /* int */
	SPGCF                  /* double */
	SPGROW                 /* uint32_t, double */
	SPMERGE                /* int */
	SPMERGEWM              /* uint32_t */
	/* db related */
	SPMERGEFORCE
	/* unrelated */
	SPVERSION /* uint32_t*, uint32_t* */
)

type spflags int

const (
	SPO_RDONLY spflags = 1
	SPO_RDWR   spflags = 2
	SPO_CREAT  spflags = 4
	SPO_SYNC   spflags = 8
)

type sporder int

const (
	SPGT sporder = iota
	SPGTE
	SPLT
	SPLTE
)

type spstat struct {
	epoch      uint32
	psn        uint64
	repn       uint32
	repndb     uint32
	repnxfer   uint32
	catn       uint32
	index      uint32
	indexpages uint32
}

type spmagic uint32

const (
	SPMCUR  spmagic = 0x15481936
	SPMENV  spmagic = 0x06154834
	SPMDB   spmagic = 0x00fec0fe
	SPMNONE spmagic = 0
)
