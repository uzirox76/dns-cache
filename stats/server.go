package stats

import (
	"encoding/json"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type Server struct {
	socketPath string
	store      Store
	startedAt  time.Time
	mu         sync.Mutex
	listener   net.Listener
}

func New(socketPath string, store Store) *Server {
	return &Server{
		socketPath: socketPath,
		store:      store,
		startedAt:  time.Now(),
	}
}

func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	os.Remove(s.socketPath)

	addr, err := net.ResolveUnixAddr("unix", s.socketPath)
	if err != nil {
		return err
	}

	l, err := net.ListenUnix("unix", addr)
	if err != nil {
		return err
	}

	if err := os.Chmod(s.socketPath, 0666); err != nil {
		l.Close()
		return err
	}

	s.listener = l

	go s.acceptLoop()
	return nil
}

func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		s.listener.Close()
		os.Remove(s.socketPath)
	}
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	snap := BuildSnapshot(s.store, s.startedAt)
	data, _ := json.MarshalIndent(snap, "", "  ")
	data = append(data, '\n')
	if _, err := conn.Write(data); err != nil {
		log.Printf("[stats] write error: %v", err)
	}
}
