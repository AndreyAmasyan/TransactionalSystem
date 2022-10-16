package app

import (
	"encoding/json"
	"net/http"
	"server/internal/manager"
	"server/internal/model"
	"server/internal/storage"
	"server/internal/utils"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

type Server struct {
	config       *Config
	logger       *logrus.Logger
	router       *mux.Router
	storage      *storage.Storage
	sync_manager *manager.SyncManager
	tasks        chan int
}

func New(config *Config) *Server {
	return &Server{
		config:       config,
		logger:       logrus.New(),
		router:       mux.NewRouter(),
		sync_manager: manager.New(),
		tasks:        make(chan int, 100),
	}
}

func (s *Server) Start() error {
	if err := s.configureLogger(); err != nil {
		return err
	}

	s.configureRouter()

	if err := s.configureStorage(); err != nil {
		return err
	}

	s.logger.Info("starting server")

	s.startWorkers(10)

	if err := s.executeTasksInQueue(); err != nil {
		return err
	}

	return http.ListenAndServe(s.config.BindAddr, s.router)
}

func (s *Server) executeTasksInQueue() error {
	count, err := s.storage.QueueTasksCount()
	if err != nil {
		return err
	}

	if count == 0 {
		return nil
	}

	tasks, err := s.storage.GetQueueTasks()
	if err != nil {
		return err
	}

	for _, v := range tasks {
		for j := 0; j < v.Count; j++ {
			s.tasks <- v.ID
		}
	}

	return nil
}

func (s *Server) startWorkers(n int) {

	for i := 0; i < n; i++ {
		go s.worker()
	}

}

func (s *Server) worker() {

	for {
		id := <-s.tasks
		if err := s.storage.UpdateBalance(id); err != nil {
			s.logger.Error(err)
			if err.Error() == utils.ErrNoMoney {
				continue
			} else {
				s.tasks <- id
			}
		}
	}

}

func (s *Server) configureLogger() error {
	level, err := logrus.ParseLevel(s.config.LogLevel)
	if err != nil {
		return nil
	}

	s.logger.SetLevel(level)

	return nil
}

func (s *Server) configureRouter() {
	s.router.HandleFunc("/updateBalance", s.handleUpdateBalance())
}

func (s *Server) configureStorage() error {
	st := storage.New(s.config.Storage)
	if err := st.Open(); err != nil {
		return err
	}

	s.storage = st

	return nil
}

func (s *Server) handleUpdateBalance() http.HandlerFunc {
	type request struct {
		ID     int `json:"id"`
		Amount int `json:"amount"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		req := &request{}
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			s.error(w, r, http.StatusBadRequest, err)
			return
		}

		p := &model.Payment{
			Account_id: req.ID,
			Amount:     req.Amount,
		}

		queue_chan := s.sync_manager.GetChan(p.Account_id)
		queue_chan <- true

		if err := s.storage.QueuePush(p); err != nil {
			s.error(w, r, http.StatusUnprocessableEntity, err)
			return
		}

		s.tasks <- p.Account_id

		<-queue_chan

		s.respond(w, r, http.StatusOK, p)
	}
}

func (s *Server) error(w http.ResponseWriter, r *http.Request, code int, err error) {
	s.respond(w, r, code, map[string]string{"error": err.Error()})
}

func (s *Server) respond(w http.ResponseWriter, r *http.Request, code int, data interface{}) {
	w.WriteHeader(code)
	if data != nil {
		json.NewEncoder(w).Encode(data)
	}
}
