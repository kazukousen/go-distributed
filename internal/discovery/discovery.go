package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type Membership struct {
	logger  *zap.Logger
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event

	nodeName       string
	bindAddr       string
	tags           map[string]string
	startJoinAddrs []string
}

type Handler interface {
	Join(id, addr string) error
	Leave(id string) error
}

func New(handler Handler, nodeName string, bindAddr string, tags map[string]string, startJoinAddrs []string) (*Membership, error) {
	m := &Membership{
		logger:         zap.L().Named("membership"),
		handler:        handler,
		nodeName:       nodeName,
		bindAddr:       bindAddr,
		tags:           tags,
		startJoinAddrs: startJoinAddrs,
	}

	return m, m.setup()
}

func (m *Membership) setup() error {
	addr, err := net.ResolveTCPAddr("tcp", m.bindAddr)
	if err != nil {
		return err
	}
	cfg := serf.DefaultConfig()
	cfg.Init()
	cfg.MemberlistConfig.BindAddr = addr.IP.String()
	cfg.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	cfg.EventCh = m.events
	cfg.Tags = m.tags
	cfg.NodeName = m.nodeName
	m.serf, err = serf.Create(cfg)
	if err != nil {
		return err
	}

	go m.eventHandler()

	if m.startJoinAddrs != nil {
		_, err = m.serf.Join(m.startJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, mem := range e.(serf.MemberEvent).Members {
				if m.isLocal(mem) {
					continue
				}
				m.handleJoin(mem)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, mem := range e.(serf.MemberEvent).Members {
				if m.isLocal(mem) {
					continue
				}
				m.handleLeave(mem)
			}
		}
	}
}

func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

func (m *Membership) Leave() error {
	return m.serf.Leave()
}

func (m *Membership) isLocal(mem serf.Member) bool {
	return m.serf.LocalMember().Name == mem.Name
}

func (m *Membership) handleJoin(mem serf.Member) {
	if err := m.handler.Join(mem.Name, mem.Tags["rpc_addr"]); err != nil {
		m.logError(err, "failed to join", mem)
	}
}

func (m *Membership) handleLeave(mem serf.Member) {
	if err := m.handler.Leave(mem.Name); err != nil {
		m.logError(err, "failed to leave", mem)
	}
}

func (m *Membership) logError(err error, msg string, mem serf.Member) {
	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", mem.Name),
		zap.String("rpc_addr", mem.Tags["rpc_addr"]),
	)
}
