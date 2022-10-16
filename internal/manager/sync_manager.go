package manager

type SyncManager struct {
	m map[int]chan bool
}

func (s *SyncManager) GetChan(id int) chan bool {
	if _, ok := s.m[id]; ok {
		return s.m[id]
	} else {
		c := make(chan bool, 1)
		return c
	}
}

func New() *SyncManager {
	return &SyncManager{}
}
