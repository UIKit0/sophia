package main

func sp_vef(e *spe, typ int, args ...interface{}) {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.typ&SPEF != 0 {
		return
	}
	e.typ = typ
	/*
		const char *fmt;
		int len;
		uint32_t epoch;
	*/

	//mask := SPEF
	//mask = ~mask
	n := typ & ^SPEF // TODO: C is ~SPEF, verify that this does what expected

	switch n {
	case SPE:
		nyi()
		/*
			fmt = va_arg(args, const char*);
			len = snprintf(e.e, sizeof(e.e), "error: ");
			vsnprintf(e.e + len, sizeof(e.e) - len, fmt, args);
		*/
	case SPEOOM:
		nyi()
		/*
			fmt = va_arg(args, const char*);
			len = snprintf(e.e, sizeof(e.e), "out-of-memory error: ");
			vsnprintf(e.e + len, sizeof(e.e) - len, fmt, args);
		*/
	case SPESYS:
		nyi()
		/*
			e.errno_ = errno;
			fmt  = va_arg(args, const char*);
			len  = snprintf(e.e, sizeof(e.e), "system error: ");
			len += vsnprintf(e.e + len, sizeof(e.e) - len, fmt, args);
			snprintf(e.e + len, sizeof(e.e) - len, " (errno: %d, %s)",
			         e.errno_, strerror(e.errno_));
		*/
	case SPEIO:
		nyi()
		/*
			e.errno_ = errno;
			epoch = va_arg(args, uint32_t);
			fmt  = va_arg(args, const char*);
			len  = snprintf(e.e, sizeof(e.e), "io error: [epoch %"PRIu32"] ", epoch);
			len += vsnprintf(e.e + len, sizeof(e.e) - len, fmt, args);
			snprintf(e.e + len, sizeof(e.e) - len, " (errno: %d, %s)",
			         e.errno_, strerror(e.errno_));
		*/
	default:
		nyi() // TODO: shouldn't happen?
	}
}
