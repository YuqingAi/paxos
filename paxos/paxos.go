package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"

type instance struct {

  a_round int
  maxn int
  maxv interface{}

  decided bool
  value interface{}
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

         // Your data here.
  instances map[int]*instance
  max int
  done map[int]int
}

type Round1Req struct {
  Seq int
  Round int
  Me int
  Done int
}

type Round1Rsp struct {
  Ok bool
  Round int
  V interface{}
}

type Round2Req struct {
  Seq int
  Round int
  Me int
  V interface{}
}

type Round2Rsp struct {
  Ok bool
}

type Round3Req struct {
  Seq int
  V interface{}
}

type Round3Rsp struct {
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    /*err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }*/
    return false
  }
  defer c.Close()

  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  /*fmt.Println(err)*/
  return false
}

func (px *Paxos) Sender(req Round3Req, rsp Round3Rsp, target int) {
  for {
    ok := call(px.peers[target], "Paxos.Learnerround3", &req, &rsp)
    if (ok) {
      return
    }
    time.Sleep(100 * time.Millisecond)
  }
}

func (px *Paxos) Proposer(seq int, v interface{}) {
  roundnum := 0

  for {
    roundnum = roundnum + 1

    maxv := interface{}(nil)
    numok := 0
    numtotal := 0
    maxround := -1
    /*if (px.me == 0 && seq == 1) {
      fmt.Println("START :")
    }*/
    for i := range px.peers {
      var ok bool
      var req1 Round1Req
      var rsp1 Round1Rsp
      req1.Done = px.done[px.me]
      req1.Me = px.me
      req1.Round = roundnum*len(px.peers) + px.me
      req1.Seq = seq
      if (i == px.me) {
        px.Acceptorround1(&req1, &rsp1)
        ok = true
      } else {
        ok = call(px.peers[i], "Paxos.Acceptorround1", &req1, &rsp1)
      }
      if (ok) {
        numtotal = numtotal + 1
        if (rsp1.Ok) {
/*          if (px.me == 0 && seq == 1) {
            fmt.Println(i)
            fmt.Println(rsp1)
          }*/
          numok = numok + 1
          if (rsp1.Round > maxround) {
            maxround = rsp1.Round
            if (rsp1.V != nil) {
              maxv = rsp1.V
            }
          }
        }
      }
    }
    if (2*numtotal <= len(px.peers)) {
      time.Sleep(100 * time.Millisecond)
      continue
    }

    if (2*numok > len(px.peers)) {

      var value interface{}
      if (maxv == nil) {
        value = v
      } else {
        value = maxv
      }

      numok := 0
      numtotal := 0
      for i := range px.peers {
        var ok bool
        var req2 Round2Req
        var rsp2 Round2Rsp
        req2.Round = roundnum*len(px.peers) + px.me
        req2.Me = px.me
        req2.Seq = seq
        req2.V = value

        if (i == px.me) {
          px.Acceptorround2(&req2, &rsp2)
          ok = true
        } else {
          ok = call(px.peers[i], "Paxos.Acceptorround2", &req2, &rsp2)
        }
        if (ok) {
          numtotal = numtotal + 1
          if (rsp2.Ok) {
            numok = numok + 1
          }
        }
      }
      /*if (px.me == 0 && seq == 1) {
        px.mu.Lock()
        fmt.Print(px.me)
        fmt.Print(" : ")
        fmt.Print(numok)
        fmt.Print(" : ")
        fmt.Println(req2.V)
        px.mu.Unlock()
      }*/
      if (2*numtotal <= len(px.peers)) {
        //v = value
        time.Sleep(100 * time.Millisecond)
        continue
      }

      if (2*numok > len(px.peers)) {

        var req3 Round3Req
        var rsp3 Round3Rsp
        req3.V = value
        req3.Seq = seq

        px.Learnerround3(&req3, &rsp3)

        for i := range px.peers {
          if (i != px.me) {
            go px.Sender(req3, rsp3, i)
          }
        }
        return
      }
    }
  }
}

func (px *Paxos) Acceptorround1(req *Round1Req, rsp *Round1Rsp) error {

  px.mu.Lock()
  if (px.done[req.Me] < req.Done) {
    px.done[req.Me] = req.Done

    min := px.Min()

    for i := range(px.instances) {
      if (i < min) {
        delete(px.instances, i)
      }
    }
  }
  px.mu.Unlock()

  ins, ok := px.instances[req.Seq]
  if (!ok) {
    px.mu.Lock()
    px.instances[req.Seq] = NewInstance()
    px.mu.Unlock()
    ins = px.instances[req.Seq]
  }

  px.mu.Lock()
  /*if (px.me == 1 && req.Seq == 1) {
    fmt.Println("Hi")
    fmt.Println(req.Round)
    fmt.Println(ins.a_round)
    fmt.Println(ins.maxv)
  }*/
  if (req.Round > ins.a_round) {
    ins.a_round = req.Round
    rsp.Round = ins.maxn
    rsp.V = ins.maxv
    /*if (px.me == 1 && req.Seq == 1) {
      fmt.Print("Res = ")
      fmt.Println(rsp.V)
    }*/
    rsp.Ok = true
  } else {
    rsp.Ok = false
  }
  /*if (px.me == 1 && req.Seq == 1) {
    fmt.Print("Res = ")
    fmt.Println(rsp)
  }*/
  px.mu.Unlock()

  return nil
}

func (px *Paxos) Acceptorround2(req *Round2Req, rsp *Round2Rsp) error {
  ins, ok := px.instances[req.Seq]
  if (!ok) {
    px.mu.Lock()
    px.instances[req.Seq] = NewInstance()
    px.mu.Unlock()
    ins = px.instances[req.Seq]
  }

  px.mu.Lock()
  if (req.Round >= ins.a_round ) {
    ins.a_round = req.Round
    ins.maxn = req.Round
    ins.maxv = req.V
    rsp.Ok = true
  } else {
    rsp.Ok = false
  }
  px.mu.Unlock()

  return nil
}

func (px *Paxos) Learnerround3(req *Round3Req, rsp *Round3Rsp) error {
  ins, ok := px.instances[req.Seq]
  if (!ok) {
    px.mu.Lock()
    px.instances[req.Seq] = NewInstance()
    px.mu.Unlock()
    ins = px.instances[req.Seq]
  }

  px.mu.Lock()
  ins.decided = true
  ins.value = req.V
  px.mu.Unlock()
  return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  px.mu.Lock()
  min := px.Min()
  px.mu.Unlock()
  if (seq < min) {
    return
  }

  px.mu.Lock()
  if (seq > px.max) {
    px.max = seq
  }
  px.mu.Unlock()

  ins, ok := px.instances[seq]
  if (!ok) {
    px.mu.Lock()
    px.instances[seq] = NewInstance()
    px.mu.Unlock()
    ins = px.instances[seq]
  }

  if (ins.decided) {
    return
  }

  go px.Proposer(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.mu.Lock()
  if (seq > px.done[px.me]) {
    px.done[px.me] = seq
  }

  min := px.Min()

  for i := range(px.instances) {
    if (i < min) {
      delete(px.instances, i)
    }
  }
  px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  return px.max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
  // You code here.
  min := 0x7ffffff
  for i := range(px.done) {
    if (px.done[i] < min) {
      min = px.done[i]
    }
  }
  return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  ins, ok := px.instances[seq]
  if (!ok || ok && !ins.decided) {
    return false, nil
  }
  return true, ins.value
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

func NewInstance() *instance {
  ins := new(instance)
  ins.decided = false
  ins.a_round = -1;
  ins.maxn = -1;
  ins.maxv = nil
  return ins
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me

  // Your initialization code here.
  px.instances = make(map[int]*instance)
  px.done = make(map[int]int)

  for i := range px.peers {
    px.done[i] = -1
  }
  px.max = -1

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }

  return px
}
