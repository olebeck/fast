package queue3

import (
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

type Client struct {
	l             sync.Mutex
	c             *websocket.Conn
	log           *logrus.Entry
	url           *url.URL
	password      string
	sessionID     uuid.UUID
	everConnected bool
}

func NewClient(uri string, sessionID uuid.UUID) (*Client, error) {
	_uri, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	var password string
	if _uri.User != nil {
		password = _uri.User.Username()
		_uri.User = nil
	}
	return &Client{
		log:       logrus.WithField("part", "Queue3Client"),
		url:       _uri,
		password:  password,
		sessionID: sessionID,
	}, nil
}

func (q *Client) connect() (err error) {
	if q.c != nil {
		q.c.Close()
		q.c = nil
	}
	var res *http.Response
	h := http.Header{}
	h.Add("X-session", q.sessionID.String())
	h.Add("X-password", q.password)
retry:
	q.c, res, err = websocket.DefaultDialer.Dial(q.url.String(), h)
	if err != nil {
		if q.everConnected {
			q.log.Errorf("Connect: %s", err)
			time.Sleep(10 * time.Second)
			goto retry
		}
		return err
	}
	_ = res
	q.everConnected = true
	return nil
}

func (q *Client) Push(data []byte) error {
	q.l.Lock()
	defer q.l.Unlock()
retry:
	if q.c == nil {
		err := q.connect()
		if err != nil {
			return err
		}
	}
	err := q.c.WriteMessage(websocket.BinaryMessage, data)
	if err != nil {
		q.log.Errorf("Push: %s", err)
		q.c = nil
		time.Sleep(10 * time.Second)
		goto retry
	}
	return nil
}

type pushData struct {
	Session uuid.UUID
	Data    []byte
}

type Server struct {
	pushes   chan pushData
	listener net.Listener
	log      *logrus.Entry
	password string
}

func NewServer(password string) *Server {
	return &Server{
		pushes:   make(chan pushData, 1000),
		log:      logrus.WithField("part", "Queue3Server"),
		password: password,
	}
}

func (q *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	q.listener = ln
	go q.serve()
	return nil
}

func (q *Server) serve() {
	if q.listener == nil {
		panic("must listen before serving")
	}

	mux := http.NewServeMux()
	upgrader := websocket.Upgrader{}
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		sessionStr := r.Header.Get("X-session")
		session, err := uuid.Parse(sessionStr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		password := r.Header.Get("X-password")
		if password != q.password {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			q.log.Warn("Unauthorized request")
			return
		}

		conn, err := upgrader.Upgrade(w, r, r.Header)
		if err != nil {
			q.log.Error(err)
		}
		for {
			msgType, data, err := conn.ReadMessage()
			if err != nil {
				q.log.Warn(err)
				return
			}
			if msgType != websocket.BinaryMessage {
				q.log.Warn("non binary message")
				return
			}
			q.pushes <- pushData{
				Session: session,
				Data:    data,
			}
		}
	})

	defer q.listener.Close()
	http.Serve(q.listener, mux)
}

func (q *Server) Address() string {
	return q.listener.Addr().String()
}

func (q *Server) Process(proc func(sessionID uuid.UUID, data []byte)) {
	for push := range q.pushes {
		proc(push.Session, push.Data)
	}
}
