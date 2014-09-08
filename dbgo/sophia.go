package main

import "sync"

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

const (
	SPENONE = 0
	SPE     = 1
	SPEOOM  = 2
	SPESYS  = 4
	SPEIO   = 8
	SPEF    = 16
)

type spe struct {
	lock sync.Mutex
	//spspinlock lock
	typ    int
	errno_ int
	e      [256]byte
}

type spenv struct {
	m     spmagic
	e     spe
	inuse int
	//spallocf alloc;
	//void *allocarg;
	//cpm spcmpf
	//void *cmparg;
	flags     uint32
	dir       string
	merge     int
	mergewm   uint32
	page      uint32
	dbnewsize uint32
	dbgrow    float64
	gc        int
	gcfactor  float64
}
