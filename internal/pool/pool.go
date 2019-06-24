package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/waterandair/go-redis-translation/internal"
)

var ErrClosed = errors.New("redis: client is closed")
var ErrPoolTimeout = errors.New("redis: connection pool timeout")

var timers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

// Stats contains pool state information and accumulated stats.
type Stats struct {
	Hits     uint32 // number of times free connection was found in the pool
	Misses   uint32 // number of times free connection was NOT found in the pool
	Timeouts uint32 // number of times a wait timeout occurred

	TotalConns uint32 // number of total connections in the pool
	IdleConns  uint32 // number of idle connections in the pool
	StaleConns uint32 // number of stale connections removed from the pool
}

type Pooler interface {
	// 建立连接和关闭连接
	NewConn(context.Context) (*Conn, error)
	CloseConn(*Conn) error

	// 连接池存取删Conn
	Get(context.Context) (*Conn, error)
	Put(*Conn)
	Remove(*Conn)

	// 监控统计
	Len() int
	IdleLen() int
	Stats() *Stats

	// 关闭连接池
	Close() error
}

// Options 初始化配置项
type Options struct {
	Dialer  func(c context.Context) (net.Conn, error)
	OnClose func(*Conn) error

	PoolSize           int
	MinIdleConns       int // 最小空闲数
	MaxConnAge         time.Duration
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
}

type ConnPool struct {
	opt *Options // 初始化配置项

	dialErrorsNum uint32 // atomic 连接错误次数

	lastDialErrorMu sync.RWMutex
	lastDialError   error // 连接错误的最后一次的错误类型

	queue chan struct{} // 与连接池 size 一样的 channel

	connsMu      sync.Mutex
	conns        []*Conn // 建立过的所有连接 conns
	idleConns    []*Conn // 空闲的(在连接池中没有被使用的) conns
	poolSize     int
	idleConnsLen int

	stats Stats

	_closed uint32 // atomic  连接池是否被关闭
}

// ConnPool 实现 Pooler 接口
var _ Pooler = (*ConnPool)(nil)

func NewConnPool(opt *Options) *ConnPool {
	p := &ConnPool{
		opt: opt,

		queue:     make(chan struct{}, opt.PoolSize),
		conns:     make([]*Conn, 0, opt.PoolSize),
		idleConns: make([]*Conn, 0, opt.PoolSize),
	}

	// 如果配置了最小空闲数,则建立最小数的空闲连接
	for i := 0; i < opt.MinIdleConns; i++ {
		p.checkMinIdleConns()
	}

	// 定时任务，清理过期的conn
	if opt.IdleTimeout > 0 && opt.IdleCheckFrequency > 0 {
		go p.reaper(opt.IdleCheckFrequency)
	}

	return p
}

// 检查最小空闲数并初始化满足最小空闲连接数的连接
func (p *ConnPool) checkMinIdleConns() {
	if p.opt.MinIdleConns == 0 {
		return
	}
	if p.poolSize < p.opt.PoolSize && p.idleConnsLen < p.opt.MinIdleConns {
		p.poolSize++
		p.idleConnsLen++
		go p.addIdleConn()
	}
}

// 建立空闲连接
func (p *ConnPool) addIdleConn() {
	cn, err := p.newConn(context.TODO(), true)
	if err != nil {
		return
	}

	p.connsMu.Lock()
	p.conns = append(p.conns, cn)
	p.idleConns = append(p.idleConns, cn)
	p.connsMu.Unlock()
}

// NewConn 实现Pooler接口的 NewConn 方法
func (p *ConnPool) NewConn(ctx context.Context) (*Conn, error) {
	return p._NewConn(ctx, false)
}

func (p *ConnPool) _NewConn(ctx context.Context, pooled bool) (*Conn, error) {
	cn, err := p.newConn(ctx, pooled)
	if err != nil {
		return nil, err
	}

	p.connsMu.Lock()
	p.conns = append(p.conns, cn)
	if pooled {
		// If pool is full remove the cn on next Put.
		if p.poolSize >= p.opt.PoolSize {
			cn.pooled = false
		} else {
			p.poolSize++
		}
	}
	p.connsMu.Unlock()
	return cn, nil
}

func (p *ConnPool) newConn(ctx context.Context, pooled bool) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	// 判断是否一直出错,发生错误的连接数大于连接池size时,返回最后一个连接错误
	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.opt.PoolSize) {
		return nil, p.getLastDialError()
	}

	// 创建一个连接,如果发生错误:将其设置为 LastDialError,
	netConn, err := p.opt.Dialer(ctx)
	if err != nil {
		p.setLastDialError(err)
		// 当错误数超过 poolSize 后,开始重试连接
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.opt.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}

	cn := NewConn(netConn)
	cn.pooled = pooled
	return cn, nil
}

// 恢复连接
func (p *ConnPool) tryDial() {
	for {
		if p.closed() {
			return
		}

		conn, err := p.opt.Dialer(context.Background())
		if err != nil {
			p.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		// 当不在出现error时,将错误连接数设置为 0
		atomic.StoreUint32(&p.dialErrorsNum, 0)
		_ = conn.Close()
		return
	}
}

func (p *ConnPool) setLastDialError(err error) {
	p.lastDialErrorMu.Lock()
	p.lastDialError = err
	p.lastDialErrorMu.Unlock()
}

func (p *ConnPool) getLastDialError() error {
	p.lastDialErrorMu.RLock()
	err := p.lastDialError
	p.lastDialErrorMu.RUnlock()
	return err
}

// Get returns existed connection from the pool or creates a new one.
func (p *ConnPool) Get(ctx context.Context) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	//等待有空闲,通过channel方式同步，如果连接池满了，则会阻塞等待，但是不会一直阻塞，有一个超时机制
	err := p.waitTurn(ctx)
	if err != nil {
		return nil, err
	}

	for {
		p.connsMu.Lock()
		cn := p.popIdle()
		p.connsMu.Unlock()

		if cn == nil {
			break
		}

		// 取出空闲的，并判断conn是否过期
		if p.isStaleConn(cn) {
			_ = p.CloseConn(cn)
			continue
		}

		atomic.AddUint32(&p.stats.Hits, 1)
		return cn, nil
	}

	// 连接池中没有空闲连接了, misses 数加 1
	atomic.AddUint32(&p.stats.Misses, 1)

	//创建新的连接
	newcn, err := p._NewConn(ctx, true)
	if err != nil {
		p.freeTurn()
		return nil, err
	}

	return newcn, nil
}

func (p *ConnPool) getTurn() {
	p.queue <- struct{}{}
}

// p.queue是Get方法成功的时候就往channel里面写，Put方法成功就往channel里面读，释放出来位置
func (p *ConnPool) waitTurn(ctx context.Context) error {
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}

	select {
	case <-done:
		return ctx.Err()
	case p.queue <- struct{}{}:
		return nil
	default:
		timer := timers.Get().(*time.Timer)
		timer.Reset(p.opt.PoolTimeout)

		select {
		case <-done:
			// 临界点处理:防止此时正好有ticker写入了timer.C,但是select到了这一个case里面
			if !timer.Stop() {
				<-timer.C
			}
			timers.Put(timer)
			return ctx.Err()
		case p.queue <- struct{}{}:
			// 临界点处理:防止此时正好有ticker写入了timer.C,但是select到了这一个case里面
			if !timer.Stop() {
				<-timer.C
			}
			timers.Put(timer)
			return nil
		case <-timer.C:
			//超时写入机制
			timers.Put(timer)
			atomic.AddUint32(&p.stats.Timeouts, 1)
			return ErrPoolTimeout
		}
	}
}

func (p *ConnPool) freeTurn() {
	<-p.queue
}

func (p *ConnPool) popIdle() *Conn {
	if len(p.idleConns) == 0 {
		return nil
	}

	idx := len(p.idleConns) - 1
	cn := p.idleConns[idx]
	p.idleConns = p.idleConns[:idx]
	p.idleConnsLen--
	p.checkMinIdleConns()
	return cn
}

func (p *ConnPool) Put(cn *Conn) {
	if !cn.pooled {
		p.Remove(cn)
		return
	}

	p.connsMu.Lock()
	// 放入空闲conns里面来，方便Get的时候取出来
	p.idleConns = append(p.idleConns, cn)
	p.idleConnsLen++
	p.connsMu.Unlock()
	// Get成功时候是写入channel，这个时候自然是释放channel
	p.freeTurn()
}

func (p *ConnPool) Remove(cn *Conn) {
	p.removeConnWithLock(cn)
	p.freeTurn()
	_ = p.closeConn(cn)
}

func (p *ConnPool) CloseConn(cn *Conn) error {
	p.removeConnWithLock(cn)
	return p.closeConn(cn)
}

func (p *ConnPool) removeConnWithLock(cn *Conn) {
	p.connsMu.Lock()
	p.removeConn(cn)
	p.connsMu.Unlock()
}

func (p *ConnPool) removeConn(cn *Conn) {
	for i, c := range p.conns {
		if c == cn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			if cn.pooled {
				p.poolSize--
				p.checkMinIdleConns()
			}
			return
		}
	}
}

func (p *ConnPool) closeConn(cn *Conn) error {
	if p.opt.OnClose != nil {
		_ = p.opt.OnClose(cn)
	}
	return cn.Close()
}

// Len returns total number of connections.
func (p *ConnPool) Len() int {
	p.connsMu.Lock()
	n := len(p.conns)
	p.connsMu.Unlock()
	return n
}

// IdleLen returns number of idle connections.
func (p *ConnPool) IdleLen() int {
	p.connsMu.Lock()
	n := p.idleConnsLen
	p.connsMu.Unlock()
	return n
}

func (p *ConnPool) Stats() *Stats {
	idleLen := p.IdleLen()
	return &Stats{
		Hits:     atomic.LoadUint32(&p.stats.Hits),
		Misses:   atomic.LoadUint32(&p.stats.Misses),
		Timeouts: atomic.LoadUint32(&p.stats.Timeouts),

		TotalConns: uint32(p.Len()),
		IdleConns:  uint32(idleLen),
		StaleConns: atomic.LoadUint32(&p.stats.StaleConns),
	}
}

func (p *ConnPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

func (p *ConnPool) Filter(fn func(*Conn) bool) error {
	var firstErr error
	p.connsMu.Lock()
	for _, cn := range p.conns {
		if fn(cn) {
			if err := p.closeConn(cn); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	p.connsMu.Unlock()
	return firstErr
}

func (p *ConnPool) Close() error {
	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return ErrClosed
	}

	var firstErr error
	p.connsMu.Lock()
	for _, cn := range p.conns {
		if err := p.closeConn(cn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = nil
	p.poolSize = 0
	p.idleConns = nil
	p.idleConnsLen = 0
	p.connsMu.Unlock()

	return firstErr
}

// 定时清理无用的conns, 一些连接可能会被 redis server 主动断开,所以需要定时清理连接
func (p *ConnPool) reaper(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for range ticker.C {
		if p.closed() {
			break
		}
		// 回收超时连接
		n, err := p.ReapStaleConns()
		if err != nil {
			internal.Logger.Printf("ReapStaleConns failed: %s", err)
			continue
		}
		atomic.AddUint32(&p.stats.StaleConns, uint32(n))
	}
}

// 清理无用的conns
func (p *ConnPool) ReapStaleConns() (int, error) {
	var n int
	for {
		// 往 p.queue channel 里面写入一个 struct{}{} ,表示占用一个任务
		p.getTurn()

		p.connsMu.Lock()
		cn := p.reapStaleConn()
		p.connsMu.Unlock()
		// 处理完了，释放占用的channel的位置
		p.freeTurn()

		if cn != nil {
			p.closeConn(cn)
			n++
		} else {
			break
		}
	}
	return n, nil
}

func (p *ConnPool) reapStaleConn() *Conn {
	if len(p.idleConns) == 0 {
		return nil
	}

	cn := p.idleConns[0]
	if !p.isStaleConn(cn) {
		return nil
	}

	p.idleConns = append(p.idleConns[:0], p.idleConns[1:]...)
	p.idleConnsLen--
	p.removeConn(cn)

	return cn
}

// 判断是不是超时连接
func (p *ConnPool) isStaleConn(cn *Conn) bool {
	if p.opt.IdleTimeout == 0 && p.opt.MaxConnAge == 0 {
		return false
	}

	now := time.Now()
	if p.opt.IdleTimeout > 0 && now.Sub(cn.UsedAt()) >= p.opt.IdleTimeout {
		return true
	}
	if p.opt.MaxConnAge > 0 && now.Sub(cn.createdAt) >= p.opt.MaxConnAge {
		return true
	}

	return false
}
