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
// Stats 包含池状态信息和统计信息
type Stats struct {
	// 获取到空闲连接的次数
	Hits uint32 // number of times free connection was found in the pool
	// 没有获取到空闲连接的次数
	Misses uint32 // number of times free connection was NOT found in the pool
	// 超时次数
	Timeouts uint32 // number of times a wait timeout occurred

	// 池中总连接数
	TotalConns uint32 // number of total connections in the pool
	// 池中空闲连接数
	IdleConns uint32 // number of idle connections in the pool
	// 从池中移除的陈旧无用连接数
	StaleConns uint32 // number of stale connections removed from the pool
}

type Pooler interface {
	// 基础连接管理: 建立连接和关闭连接
	//    这里提供了一种仅仅是新建一个连接的方式,通过这种方式建立的连接不会被放到连接
	//    池中,使用完后要执行 CloseConn 函数关闭连接,即使调用 Put 或 Remove 函数,
	//    也仅仅相当于执行了  CloseConn 操作
	NewConn(context.Context) (*Conn, error)
	CloseConn(*Conn) error

	// 连接池连接管理: 从池中对连接进行存取和删除
	Get(context.Context) (*Conn, error)
	Put(*Conn)
	Remove(*Conn)

	// 监控统计信息
	Len() int      // 实时连接池大小
	IdleLen() int  // 实时空闲连接数大小
	Stats() *Stats // 详细状态信息(下一小节介绍)

	// 关闭连接池
	Close() error
}

// Options 初始化配置项
type Options struct {
	Dialer  func(c context.Context) (net.Conn, error)
	OnClose func(*Conn) error

	PoolSize           int           // 池大小
	MinIdleConns       int           // 最小空闲数
	MaxConnAge         time.Duration // 一个连接的最长生命时长
	PoolTimeout        time.Duration // 连接池的过期时间
	IdleTimeout        time.Duration // 一个空闲连接的过期时间
	IdleCheckFrequency time.Duration
}

type ConnPool struct {
	opt             *Options      // 配置项
	dialErrorsNum   uint32        // 建立连接错误次数(原子操作)
	lastDialErrorMu sync.RWMutex  // 并发操作 lastDialError 的读写锁
	lastDialError   error         // 最后一次建立连接发生的错误
	queue           chan struct{} // 与连接池 size 一样的 channel, 当没有空闲连接时,用于阻塞获取连接的请求
	connsMu         sync.Mutex    // 并发操作 conns,idleConns 的互斥锁
	conns           []*Conn       // 连接池 conns
	idleConns       []*Conn       // 空闲的(在连接池中没有被使用的) conns
	poolSize        int           // 实时连接池大小
	idleConnsLen    int           // 实时空闲连接数
	stats           Stats         // 连接池状态信息
	_closed         uint32        // 连接池是否被关闭(原子操作)
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

	// 检查最小空闲连接: 如果配置了最小空闲数,则建立最小数的空闲连接
	for i := 0; i < opt.MinIdleConns; i++ {
		// 空闲连接数小于配置时,每次执行check只会新建一个连接
		p.checkMinIdleConns()
	}

	// 清理过期 conn
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
	// 当前池的总连接数在 opt.PoolSize 内的前提下,需要考虑最小空闲连接数的情况
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

// NewConn 实现Pooler接口的 NewConn 方法, 新建一个连接
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
		// 当错误数等于 poolSize 后,开始重试连接
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.opt.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}

	cn := NewConn(netConn) // todo
	cn.pooled = pooled
	return cn, nil
}

// 重试连接,如果连接出错,则每秒钟尝试新建一个连接,直到连接建立成功
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

	// 等待有空闲,通过channel方式同步，如果连接池满了，则会阻塞等待，但是不会一直阻塞，有一个超时机制, ctx 中要包含超时条件
	err := p.waitTurn(ctx)
	if err != nil {
		return nil, err
	}

	// 从空闲 conns 中获取 conn
	for {
		p.connsMu.Lock()
		cn := p.popIdle()
		p.connsMu.Unlock()

		if cn == nil {
			break
		}

		// 判断conn是否过期
		if p.isStaleConn(cn) {
			_ = p.CloseConn(cn)
			// 关闭超时 conn 后继续获取
			continue
		}

		atomic.AddUint32(&p.stats.Hits, 1)
		return cn, nil
	}

	// 连接池中没有空闲连接了, 需要新建连接

	// misses 数加 1
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

// p.queue 是 Get方法成功的时候就往channel里面写，Put方法成功就往channel里面读，释放出来位置
func (p *ConnPool) waitTurn(ctx context.Context) error {
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}

	select {
	case <-done:
		// 在调用 Get 方法时在 ctx 中设置的超时机制
		return ctx.Err()
	case p.queue <- struct{}{}:
		return nil
	default:
		// 默认的超时机制
		timer := timers.Get().(*time.Timer)
		// 从临时对象池中获取一个 timer, 用于设置连接池的超时时间
		timer.Reset(p.opt.PoolTimeout)

		// 双重检查
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
	// 去最后一个空闲连接
	idx := len(p.idleConns) - 1
	cn := p.idleConns[idx]
	p.idleConns = p.idleConns[:idx]
	p.idleConnsLen--
	// 每获取一个连接,就检查一次最少空闲连接数
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
	// Get成功时候是写入channel，Put 成功后释放channel
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

// 判断连接池是否已被关闭
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
		// 从空闲连接中，回收超时连接
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
		// 往 p.queue channel 里面写入一个 struct{}{} ,表示占用一个任务,准备读取一个空闲连接
		p.getTurn()

		p.connsMu.Lock()
		cn := p.reapStaleConn()
		p.connsMu.Unlock()
		// 处理完了，释放占用的channel的位置,表示读取空闲连接结束
		p.freeTurn()

		// todo 这里会不会发生遇到一个未超时连接,就返回nil, 停止检查后面的连接, idleConns 是不是保证超时的连接都在 slice 的前面
		if cn != nil {
			p.closeConn(cn)
			n++
		} else {
			break
		}
	}
	return n, nil
}

// 清理无用连接
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
