// suitable for asynchrous tcp protobuf message
// message format is msglen(4byte) + cmd(1byte) + marshaled msg
package mux

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
)

type MarshalToer interface {
	Size() (n int)
	MarshalTo(data []byte) (int, error)
}

type UnMarshaler interface {
	Unmarshal(data []byte) error
}

const (
	defInChannelSize = 1024
)

const (
	defaultConnTimeout  = 2 * time.Second
	defaultReadTimeout  = time.Second
	defaultWriteTimeout = time.Second
)

var (
	ErrMuxAssertFailed    = errors.New("assert failed")
	ErrSessionReadTimeout = errors.New("session read timeout")
)

type ProtoMessage struct {
	Cmd uint8
	Msg interface{}
	sid int64
}

type TcpProtoSession struct {
	Sessionid    int64
	Out          chan *ProtoMessage
	mux          *TcpProtoMux
	readTimeout  time.Duration
	writeTimeout time.Duration
}

type MuxProtoHandler interface {
	Parse(cmd uint8, buf []byte) (int64, interface{}, error)
	ReplyWhenSendErr() interface{}
}

type TcpProtoMux struct {
	conn         net.Conn
	url          string
	seedsid      int64
	mu           *sync.RWMutex
	in           chan *ProtoMessage
	inChanSize   int
	sessionMap   map[int64]*TcpProtoSession
	protoHandler MuxProtoHandler
	connTimeout  time.Duration
}

func (m *TcpProtoMux) startMultiplexerSendDaemon() {
	go func() {
		logger := log.WithField("server_url", m.url)
		for {
			protoMsg := <-m.in
			m.mu.RLock()
			conn := m.conn
			m.mu.RUnlock()
			err := SendMsg(conn, protoMsg.Cmd, protoMsg.Msg)
			if err != nil {
				if err == io.EOF {
					logger.Info("connection closed. retry once")
				} else {
					logger.Errorf("sendMsg error:%v. retry once", err)
				}
				m.mu.Lock()
				if m.conn != nil {
					m.conn.Close()
				}
				m.conn, err = net.DialTimeout("tcp", m.url, time.Duration(m.connTimeout))
				m.mu.Unlock()
				if err == nil {
					logger.Debugf("sendside dial url:%s successfully", m.url)
					err = SendMsg(m.conn, protoMsg.Cmd, protoMsg.Msg)
				} else {
					logger.Errorf("sendside dial url:%s error:%v", m.url, err)
				}
			}

			if err != nil {
				reply := m.protoHandler.ReplyWhenSendErr()
				m.replyToSession(protoMsg.sid, protoMsg.Cmd, reply)
				continue
			}
		}
	}()
}

func (m *TcpProtoMux) replyToSession(sid int64, cmd uint8, reply interface{}) error {
	logger := log.WithField("server_url", m.url)
	protoMsg := &ProtoMessage{
		Cmd: cmd,
		Msg: reply,
		sid: sid,
	}

	m.mu.RLock()
	session, ok := m.sessionMap[sid]
	m.mu.RUnlock()
	if ok {
		session.Out <- protoMsg
	} else {
		logger.Errorf("no session id[%d] found. maybe removed for timeout", sid)
	}

	return nil
}

func (m *TcpProtoMux) dispatchReplyToSession(cmd uint8, buf []byte) error {
	logger := log.WithField("server_url", m.url)
	protoMsg := &ProtoMessage{
		Cmd: cmd,
	}

	sid, msg, err := m.protoHandler.Parse(cmd, buf)
	if err != nil {
		logger.Errorf("proto parse error:%v", err)
		return err
	}
	protoMsg.Msg = msg

	m.replyToSession(sid, protoMsg.Cmd, protoMsg.Msg)

	return nil
}

func (m *TcpProtoMux) startMultiplexerRecvDaemon() {
	go func() {
		logger := log.WithField("server_url", m.url)
		for {
			m.mu.RLock()
			conn := m.conn
			m.mu.RUnlock()
			cmd, bin, err := RecvMsg(conn)
			if err != nil {
				if err == io.EOF {
					logger.Info("connection closed. retry once")
				} else {
					logger.Errorf("recvMsg error:%v. retry", err)
				}
				m.mu.Lock()
				if m.conn != nil {
					m.conn.Close()
				}
				m.conn, err = net.DialTimeout("tcp", m.url, time.Duration(m.connTimeout))
				m.mu.Unlock()
				if err != nil {
					logger.Errorf("recvside dial url:%s error:%v", m.url, err)
				} else {
					logger.Debugf("recvside dial url:%s successfully", m.url)
				}

				continue
			}

			err = m.dispatchReplyToSession(cmd, bin)
			if err != nil {
				logger.Errorf("dispatch reply to session error:%v", err)
				continue
			}
		}
	}()
}

func (m *TcpProtoMux) StartMux() error {
	m.startMultiplexerSendDaemon()
	m.startMultiplexerRecvDaemon()

	return nil
}

func (m *TcpProtoMux) NewSession() *TcpProtoSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	sid := atomic.LoadInt64(&m.seedsid)
	atomic.AddInt64(&m.seedsid, 1)
	session := &TcpProtoSession{
		Sessionid:    sid,
		Out:          make(chan *ProtoMessage, 1),
		mux:          m,
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
	}
	m.sessionMap[sid] = session

	return session
}

func (s *TcpProtoSession) SetReadTimeout(timeout time.Duration) {
	s.readTimeout = timeout
}

func (s *TcpProtoSession) SetWriteTimeout(timeout time.Duration) {
	s.writeTimeout = timeout
}

func (s *TcpProtoSession) Send(cmd uint8, v interface{}) error {
	if s.mux == nil {
		return ErrMuxAssertFailed
	}

	protoMsg := &ProtoMessage{
		Cmd: cmd,
		Msg: v,
		sid: s.Sessionid,
	}
	s.mux.in <- protoMsg

	return nil
}

func (s *TcpProtoSession) Recv() (cmd uint8, v interface{}, err error) {
	cmd = 0
	v = nil
	if s.mux == nil {
		err = ErrMuxAssertFailed
		return
	}

	select {
	case protoMsg := <-s.Out:
		if protoMsg != nil {
			cmd = protoMsg.Cmd
			v = protoMsg.Msg
		}
	case <-time.After(s.readTimeout):
		log.WithField("sid", s.Sessionid).Infof("session read timeout")
		err = ErrSessionReadTimeout
	}
	s.close()

	return
}

func (s *TcpProtoSession) RemoteUrl() string {
	return s.mux.url
}

func (s *TcpProtoSession) close() error {
	if s.mux == nil {
		return ErrMuxAssertFailed
	}

	// lock, and remove session. so it is safe to close chan in send side
	s.mux.mu.Lock()
	defer s.mux.mu.Unlock()

	delete(s.mux.sessionMap, s.Sessionid)
	close(s.Out)

	return nil
}

func NewTcpProtoMux(conn net.Conn, protoHandler MuxProtoHandler,
	concurrentSize int, connTimeout time.Duration) *TcpProtoMux {
	if protoHandler == nil {
		return nil
	}

	if connTimeout == time.Duration(0) {
		connTimeout = defaultConnTimeout
	}
	if concurrentSize <= 0 {
		concurrentSize = defInChannelSize
	}

	tcpProtoMux := &TcpProtoMux{
		conn:         conn,
		url:          conn.RemoteAddr().String(),
		seedsid:      1,
		mu:           new(sync.RWMutex),
		in:           make(chan *ProtoMessage, concurrentSize),
		sessionMap:   make(map[int64]*TcpProtoSession),
		protoHandler: protoHandler,
		connTimeout:  connTimeout,
	}

	err := tcpProtoMux.StartMux()
	if err != nil {
		log.WithField("server_url", tcpProtoMux.url).Errorf("start mux error:%v", err)
		return nil
	}

	return tcpProtoMux
}

func SendMsg(conn net.Conn, cmd uint8, v interface{}) error {
	if conn == nil {
		return errors.New("nil conn")
	}

	request, ok := v.(MarshalToer)
	if !ok {
		return errors.New("not support UnmarshalTo")
	}

	length := 4
	msglen := 1 + request.Size()
	length += msglen
	bin := make([]byte, length)

	binary.LittleEndian.PutUint32(bin, uint32(msglen))
	bin[4] = byte(cmd) //
	mlen, err := request.MarshalTo(bin[5:])
	if err != nil {
		log.Errorf("marshal error:%v", err)
		return err
	} else if mlen != (msglen - 1) {
		log.Errorf("marshal len:%d not same as size:%d", mlen, request.Size())
	}

	_, err = conn.Write(bin)
	if err != nil {
		log.Errorf("write error:%v", err)
	}

	return err
}

func RecvMsg(conn net.Conn) (cmd uint8, bin []byte, err error) {
	cmd = 0
	bin = nil
	if conn == nil {
		err = errors.New("nil conn")
		return
	}

	var lenBuf [4]byte
	// conn can be reused. do NOT set deadline
	_, err = io.ReadFull(conn, lenBuf[:])
	if err != nil {
		if err == io.EOF {
			log.Info("readfull eof. will retry")
		}
		return
	}

	err = nil
	msglen := binary.LittleEndian.Uint32(lenBuf[:])
	msgBuf := make([]byte, msglen)
	io.ReadFull(conn, msgBuf)
	cmd = msgBuf[0]
	bin = msgBuf[1:]

	return
}
