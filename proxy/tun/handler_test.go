package tun

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
)

type testCounter struct {
	value int64
}

func (c *testCounter) Value() int64 {
	return atomic.LoadInt64(&c.value)
}

func (c *testCounter) Set(value int64) int64 {
	return atomic.SwapInt64(&c.value, value)
}

func (c *testCounter) Add(value int64) int64 {
	return atomic.AddInt64(&c.value, value) - value
}

type testConn struct {
	reader *bytes.Reader
	writer bytes.Buffer
}

func newTestConn(input []byte) *testConn {
	return &testConn{reader: bytes.NewReader(input)}
}

func (c *testConn) Read(payload []byte) (int, error) {
	return c.reader.Read(payload)
}

func (c *testConn) Write(payload []byte) (int, error) {
	return c.writer.Write(payload)
}

func (c *testConn) Close() error {
	return nil
}

func (c *testConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 1080}
}

func (c *testConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 12345}
}

func (c *testConn) SetDeadline(time.Time) error {
	return nil
}

func (c *testConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *testConn) SetWriteDeadline(time.Time) error {
	return nil
}

type testDispatcher struct {
	writePayload []byte
	readBytes    int32
}

func (d *testDispatcher) Type() interface{} {
	return routing.DispatcherType()
}

func (d *testDispatcher) Start() error {
	return nil
}

func (d *testDispatcher) Close() error {
	return nil
}

func (d *testDispatcher) Dispatch(context.Context, xnet.Destination) (*transport.Link, error) {
	return nil, nil
}

func (d *testDispatcher) DispatchLink(ctx context.Context, dest xnet.Destination, link *transport.Link) error {
	mb, err := link.Reader.ReadMultiBuffer()
	if err != nil {
		return err
	}
	atomic.StoreInt32(&d.readBytes, mb.Len())
	buf.ReleaseMulti(mb)

	return link.Writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(d.writePayload)})
}

type holdingDispatcher struct {
	testDispatcher
	release chan struct{}
	uplink  chan []byte
}

func (d *holdingDispatcher) DispatchLink(ctx context.Context, dest xnet.Destination, link *transport.Link) error {
	if err := d.testDispatcher.DispatchLink(ctx, dest, link); err != nil {
		return err
	}
	if err := common.Close(link.Writer); err != nil {
		return err
	}
	reader := &buf.BufferedReader{Reader: link.Reader}
	payload := make([]byte, len("after EOF"))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return err
	}
	d.uplink <- payload
	<-d.release // like an outbound waiting for uplinkOnly
	return nil
}

func TestHandlerHalfClosesTunConnectionWhenResponseEnds(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	common.Must(err)
	defer listener.Close()
	app, err := net.Dial("tcp", listener.Addr().String())
	common.Must(err)
	defer app.Close()
	conn, err := listener.Accept()
	common.Must(err)

	release := make(chan struct{})
	uplink := make(chan []byte, 1)
	done := make(chan struct{})
	defer func() {
		close(release)
		app.Close()
		conn.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("handler did not finish")
		}
	}()
	handler := &Handler{
		ctx:             context.Background(),
		config:          &Config{},
		dispatcher:      &holdingDispatcher{testDispatcher: testDispatcher{writePayload: []byte("downlink")}, release: release, uplink: uplink},
		uplinkCounter:   new(testCounter),
		downlinkCounter: new(testCounter),
	}
	go func() {
		defer close(done)
		handler.HandleConnection(conn, xnet.TCPDestination(xnet.LocalHostIP, 443))
	}()

	common.Must2(app.Write([]byte("uplink")))
	common.Must(app.SetReadDeadline(time.Now().Add(time.Second)))
	got, err := io.ReadAll(app)
	if err != nil || string(got) != "downlink" {
		t.Fatalf("unexpected response: got %q, %v, want %q and EOF", got, err, "downlink")
	}
	common.Must(app.SetWriteDeadline(time.Now().Add(time.Second)))
	common.Must2(app.Write([]byte("after EOF")))
	select {
	case payload := <-uplink:
		if string(payload) != "after EOF" {
			t.Fatalf("unexpected uplink after EOF: %q", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("uplink stopped after response EOF")
	}
	select {
	case <-done:
		t.Fatal("handler ended before the uplink grace period")
	default:
	}
	if got := handler.uplinkCounter.Value(); got != int64(len("uplinkafter EOF")) {
		t.Fatalf("unexpected uplink counter after EOF: %d", got)
	}
}

func TestHandlerCountsTunConnectionTraffic(t *testing.T) {
	uplinkCounter := new(testCounter)
	downlinkCounter := new(testCounter)
	dispatcher := &testDispatcher{writePayload: []byte("downlink")}
	conn := newTestConn([]byte("uplink"))

	handler := &Handler{
		ctx:             context.Background(),
		config:          &Config{},
		dispatcher:      dispatcher,
		uplinkCounter:   uplinkCounter,
		downlinkCounter: downlinkCounter,
	}
	handler.HandleConnection(conn, xnet.TCPDestination(xnet.LocalHostIP, 443))

	if got := uplinkCounter.Value(); got != int64(len("uplink")) {
		t.Fatalf("unexpected uplink counter: got %d, want %d", got, len("uplink"))
	}
	if got := downlinkCounter.Value(); got != int64(len("downlink")) {
		t.Fatalf("unexpected downlink counter: got %d, want %d", got, len("downlink"))
	}
	if got := int(atomic.LoadInt32(&dispatcher.readBytes)); got != len("uplink") {
		t.Fatalf("dispatcher read unexpected bytes: got %d, want %d", got, len("uplink"))
	}
	if got := conn.writer.String(); got != "downlink" {
		t.Fatalf("connection write mismatch: got %q, want %q", got, "downlink")
	}
}
