package a

type live interface{ CheckLive() }

type holder struct{ dep any }

func (h holder) bad() { h.dep.(live).CheckLive() }

func (h holder) good() {
	if d, ok := h.dep.(live); ok {
		d.CheckLive()
	}
}

func (h holder) notes() {
	_ = h.dep.(*holder)
	_ = h.dep.(string)
	//assertcheck:ok the value was built two lines up
	_ = h.dep.(live)
}
