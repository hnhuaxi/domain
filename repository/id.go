package repository

type Key interface {
	Name() string
	Value() interface{}
}

type uid struct {
	val uint
}

func ID(id uint) Key {
	return uid{
		val: id,
	}
}

func ID32(id uint32) Key {
	return uid{
		val: uint(id),
	}
}

func (u uid) Name() string {
	return "id"
}

func (u uid) Value() interface{} {
	return u.val
}

type sid struct {
	id string
}

func SID(id string) Key {
	return sid{
		id: id,
	}
}

func (u sid) Name() string {
	return "id"
}

func (u sid) Value() interface{} {
	return u.id
}

type fullkey struct {
	id  string
	val interface{}
}

func FullID(id string, val interface{}) Key {
	return &fullkey{
		id:  id,
		val: val,
	}
}

func (full *fullkey) Name() string {
	return full.id
}

func (full *fullkey) Value() interface{} {
	return full.val
}
