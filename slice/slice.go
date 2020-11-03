package slice

func Allocate(in []byte, n int) ([]byte, []byte) {
	if nn := len(in) + n; cap(in) >= nn {
		return in[:nn], in[len(in):nn]
	}
	in = make([]byte, n, 2*n+512)
	return in, in
}

func ForAppend(in []byte, n int) (head, tail []byte) {
	if nn := len(in) + n; cap(in) >= nn {
		head = in[:nn]
	} else {
		head = make([]byte, nn, 2*nn)
		copy(head, in)
	}
	tail = head[len(in):]
	return
}

// Insert inserts n bytes at position pos in a byte slice
func Insert(in []byte, pos, n int) []byte {
	if n == 0 {
		return in
	}
	if pos+n <= cap(in) {
		return append(in[:pos+n], in[pos:]...)
	}
	p := make([]byte, len(in)+n, len(in)+n+512)
	copy(p, in[:pos])
	copy(p[pos+n:], in[pos:])
	return p
}
