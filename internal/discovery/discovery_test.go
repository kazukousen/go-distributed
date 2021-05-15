package discovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

const (
	testEventuallyWaitFor  = 3 * time.Second
	testEventuallyInterval = 250 * time.Millisecond
)

func TestMembership(t *testing.T) {
	m, handler := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	require.Eventually(t, func() bool {
		return 2 == len(handler.joins) &&
			3 == len(m[0].Members()) &&
			0 == len(handler.leaves)
	}, testEventuallyWaitFor, testEventuallyInterval)

	require.NoError(t, m[2].Leave())

	require.Eventually(t, func() bool {
		return 2 == len(handler.joins) &&
			3 == len(m[0].Members()) &&
			serf.StatusLeft == m[0].Members()[2].Status &&
			1 == len(handler.leaves)
	}, testEventuallyWaitFor, testEventuallyInterval)

	require.Equal(t, fmt.Sprintf("%d", 2), <-handler.leaves)
}

func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	id := len(members)
	ports := dynaport.Get(1)
	addr := fmt.Sprintf("127.0.0.1:%d", ports[0])
	tags := map[string]string{
		"rpc_addr": addr,
	}

	h := &handler{}
	startJoinAddrs := ([]string)(nil)
	if len(members) == 0 {
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		startJoinAddrs = []string{
			members[0].bindAddr,
		}
	}

	m, err := New(h, fmt.Sprintf("%d", id), addr, tags, startJoinAddrs)
	require.NoError(t, err)
	members = append(members, m)

	return members, h
}

type handler struct {
	joins  chan map[string]string
	leaves chan string
}

var _ Handler = (*handler)(nil)

func (h *handler) Join(id, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}
	return nil
}

func (h *handler) Leave(id string) error {
	if h.leaves != nil {
		h.leaves <- id
	}

	return nil
}
