package replica

import (
	"encoding/gob"
	"fmt"
	"github.com/gitferry/bamboo/EncBroadCast"
	"github.com/gitferry/bamboo/newConsensus"
	"github.com/gitferry/bamboo/newStreamlet"
	//fhs "github.com/gitferry/bamboo/fasthostuff"
	//"github.com/gitferry/bamboo/lbft"
	"time"

	"go.uber.org/atomic"

	"github.com/gitferry/bamboo/blockchain"
	"github.com/gitferry/bamboo/config"
	"github.com/gitferry/bamboo/election"
	//"github.com/gitferry/bamboo/hotstuff"
	"github.com/gitferry/bamboo/identity"
	"github.com/gitferry/bamboo/log"
	"github.com/gitferry/bamboo/mempool"
	"github.com/gitferry/bamboo/message"
	"github.com/gitferry/bamboo/node"
	"github.com/gitferry/bamboo/pacemaker"
	"github.com/gitferry/bamboo/streamlet"
	//"github.com/gitferry/bamboo/tchs"
	"github.com/gitferry/bamboo/types"
)

type Replica struct {
	node.Node
	Safety
	election.Election
	enc             *EncBroadCast.EncBroadcast
	pd              *mempool.Producer
	pm              *pacemaker.Pacemaker
	start           chan bool // signal to start the node
	isStarted       atomic.Bool
	isByz           bool
	timer           *time.Timer // timeout for each view
	committedBlocks chan *blockchain.Block
	forkedBlocks    chan *blockchain.Block
	eventChan       chan interface{}
	alg             string
	//ComiitedMessage blockchain.CommitMessages
	/* for monitoring node statistics */
	thrus                string
	lastViewTime         time.Time
	startTime            time.Time
	tmpTime              time.Time
	voteStart            time.Time
	totalCreateDuration  time.Duration
	totalProcessDuration time.Duration
	totalProposeDuration time.Duration
	totalDelay           int64
	totalRoundTime       time.Duration
	totalVoteTime        time.Duration
	totalBlockSize       int
	receivedNo           int
	roundNo              int
	voteNo               int
	totalCommittedTx     int
	latencyNo            int
	proposedNo           int
	processedNo          int
	committedNo          int
	endTarget            bool
}

// NewReplica creates a new replica instance
func NewReplica(id identity.NodeID, alg string, isByz bool) *Replica {
	r := new(Replica)
	r.Node = node.NewNode(id, isByz)
	if isByz {
		log.Infof("[%v] is Byzantine", r.ID())
	}
	if config.GetConfig().Master == "0" {
		r.Election = election.NewRotation(config.GetConfig().N())
	} else if config.GetConfig().Master == "-1" {
		r.Election = election.NewCsHRotation(config.GetConfig().N())
	} else {
		r.Election = election.NewStatic(config.GetConfig().Master)
	}
	r.isByz = isByz
	r.endTarget = false
	r.pd = mempool.NewProducer()
	r.pm = pacemaker.NewPacemaker(config.GetConfig().N())
	//log.Debug("init0 view is ", r.pm.GetCurView())
	r.start = make(chan bool)
	r.eventChan = make(chan interface{})
	r.committedBlocks = make(chan *blockchain.Block, 100)
	r.forkedBlocks = make(chan *blockchain.Block, 100)
	r.enc = EncBroadCast.NewencBroadcast(r.Node, config.GetConfig().N(), r.Election)
	//r.ComiitedMessage = make(blockchain.CommitMessages, 0)
	r.Register(blockchain.Block{}, r.HandleBlock)
	r.Register(blockchain.Vote{}, r.HandleVote)
	r.Register(pacemaker.TMO{}, r.HandleTmo)
	r.Register(message.Transaction{}, r.handleTxn)
	r.Register(message.Query{}, r.handleQuery)
	r.Register(EncBroadCast.Val{}, r.handleVal)
	r.Register(EncBroadCast.Echo{}, r.handleEcho)
	r.Register(blockchain.QC{}, r.HandleQC)
	gob.Register(blockchain.Block{})
	gob.Register(blockchain.Vote{})
	gob.Register(pacemaker.TC{})
	gob.Register(pacemaker.TMO{})
	gob.Register(EncBroadCast.Val{})
	gob.Register(blockchain.CoderBlock{})
	gob.Register(EncBroadCast.Echo{})
	gob.Register(blockchain.QC{})
	//log.Debug("init1 view is ", r.pm.GetCurView())
	// Is there a better way to reduce the number of parameters?
	r.alg = alg
	switch alg {
	//case "hotstuff":
	//	r.Safety = hotstuff.NewHotStuff(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	//case "tchs":
	//	r.Safety = tchs.NewTchs(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	case "streamlet":
		r.Safety = streamlet.NewStreamlet(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	//case "lbft":
	//	r.Safety = lbft.NewLbft(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	case "newConsensus":
		r.Safety = newConsensus.NewStreamlet(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks, r.enc)
	case "newStreamlet":
		r.Safety = newStreamlet.NewStreamlet(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks, r.enc)
	default:
		r.Safety = streamlet.NewStreamlet(r.Node, r.pm, r.Election, r.committedBlocks, r.forkedBlocks)
	}
	//log.Debug("init2 view is ", r.pm.GetCurView())

	return r
}

/* Message Handlers */
func (r *Replica) handleVal(val EncBroadCast.Val) {
	//r.startSignal()
	log.Debugf("[%v] received a val from %v, view is %v", r.ID(), val.Proposer, val.View)
	r.enc.MessageCh <- val
}

func (r *Replica) handleEcho(echo EncBroadCast.Echo) {
	//r.startSignal()
	log.Debugf("[%v] received a echo from %v, view is %v", r.ID(), echo.Proposer, echo.View)
	r.enc.MessageCh <- echo
}

func (r *Replica) HandleBlock(block blockchain.Block) {
	r.receivedNo++
	r.startSignal()
	log.Debugf("[%v] received a block from %v, view is %v, id: %x, prevID: %x", r.ID(), block.Proposer, block.View, block.ID, block.PrevID)
	r.eventChan <- block
}

func (r *Replica) HandleVote(vote blockchain.Vote) {
	r.startSignal()
	log.Debugf("[%v] received a vote frm %v, blockID is %x", r.ID(), vote.Voter, vote.BlockID)
	r.eventChan <- vote
}

func (r *Replica) HandleTmo(tmo pacemaker.TMO) {
	if tmo.View < r.pm.GetCurView() {
		return
	}
	log.Debugf("[%v] received a timeout from %v for view %v", r.ID(), tmo.NodeID, tmo.View)
	r.eventChan <- tmo
}

func (r *Replica) HandleQC(QC blockchain.QC) {
	r.startSignal()
	log.Debugf("[%v] received a QC frm %v, blockID is %x", QC.Leader, QC.BlockID)
	r.eventChan <- QC
}

// handleQuery replies a query with the statistics of the node
func (r *Replica) handleQuery(m message.Query) {
	//realAveProposeTime := float64(r.totalProposeDuration.Milliseconds()) / float64(r.processedNo)
	//aveProcessTime := float64(r.totalProcessDuration.Milliseconds()) / float64(r.processedNo)
	//aveVoteProcessTime := float64(r.totalVoteTime.Milliseconds()) / float64(r.roundNo)
	//aveBlockSize := float64(r.totalBlockSize) / float64(r.proposedNo)
	//requestRate := float64(r.pd.TotalReceivedTxNo()) / time.Now().Sub(r.startTime).Seconds()
	//committedRate := float64(r.committedNo) / time.Now().Sub(r.startTime).Seconds()
	//aveRoundTime := float64(r.totalRoundTime.Milliseconds()) / float64(r.roundNo)
	//aveProposeTime := aveRoundTime - aveProcessTime - aveVoteProcessTime
	latency := float64(r.totalDelay) / float64(r.latencyNo)
	r.thrus += fmt.Sprintf("Time: %v s. Throughput: %v txs/s\n", time.Now().Sub(r.startTime).Seconds(), float64(r.totalCommittedTx)/time.Now().Sub(r.tmpTime).Seconds())
	r.totalCommittedTx = 0
	r.tmpTime = time.Now()
	status := fmt.Sprintf("Latency: %v\n%s", latency, r.thrus)
	//status := fmt.Sprintf("chain status is: %s\nCommitted rate is %v.\nAve. block size is %v.\nAve. trans. delay is %v ms.\nAve. creation time is %f ms.\nAve. processing time is %v ms.\nAve. vote time is %v ms.\nRequest rate is %f txs/s.\nAve. round time is %f ms.\nLatency is %f ms.\nThroughput is %f txs/s.\n", r.Safety.GetChainStatus(), committedRate, aveBlockSize, aveTransDelay, aveCreateDuration, aveProcessTime, aveVoteProcessTime, requestRate, aveRoundTime, latency, throughput)
	//status := fmt.Sprintf("Ave. actual proposing time is %v ms.\nAve. proposing time is %v ms.\nAve. processing time is %v ms.\nAve. vote time is %v ms.\nAve. block size is %v.\nAve. round time is %v ms.\nLatency is %v ms.\n", realAveProposeTime, aveProposeTime, aveProcessTime, aveVoteProcessTime, aveBlockSize, aveRoundTime, latency)
	m.Reply(message.QueryReply{Info: status})
}

func (r *Replica) handleTxn(m message.Transaction) {
	//log.Debug("revc a txn %v", m)
	r.pd.AddTxn(&m)
	r.startSignal()
	// the first leader kicks off the protocol
	if r.pm.GetCurView() == 0 && r.IsLeader(r.ID(), 1) {
		log.Debugf("[%v] is going to kick off the protocol", r.ID())
		r.pm.AdvanceView(0)
	}
}

/* Processors */

func (r *Replica) processCommittedBlock(block *blockchain.Block) {
	//if block.Proposer == r.ID() {
	for _, txn := range block.Payload {
		// only record the delay of transactions from the local memory pool
		delay := time.Now().Sub(txn.Timestamp)
		r.totalDelay += delay.Milliseconds()
		r.latencyNo++
	}
	//
	r.committedNo++
	r.totalCommittedTx += len(block.Payload)
	r.UpdateBehaviour(true, false, block.Proposer)
	//r.ComiitedMessage = append(r.ComiitedMessage, &blockchain.CommitMessage{block.View, block.Proposer})
	//log.Infof("lantency is", float64(r.totalDelay.Milliseconds())/float64(r.latencyNo))
	log.Infof("[%v] the block is committed, No. of transactions: %v, view: %v, current view: %v, id: %x", r.ID(), len(block.Payload), block.View, r.pm.GetCurView(), block.ID)
}

func (r *Replica) processForkedBlock(block *blockchain.Block) {
	if block.Proposer == r.ID() {
		for _, txn := range block.Payload {
			// collect txn back to mem pool
			r.pd.CollectTxn(txn)
		}
	}
	log.Infof("[%v] the block is forked, No. of transactions: %v, view: %v, current view: %v, id: %x", r.ID(), len(block.Payload), block.View, r.pm.GetCurView(), block.ID)
}

func (r *Replica) processNewView(newView types.View) {
	log.Debugf("[%v] is processing new view: %v, leader is %v", r.ID(), newView, r.FindLeaderFor(newView))
	if newView > 10 && newView%500 == 0 {
		r.UpdateWeight(newView)
	}
	r.UpdateBehaviour(false, true, r.FindLeaderFor(newView))
	if !r.IsLeader(r.ID(), newView) {
		return
	}
	r.proposeBlock(newView)
}

func (r *Replica) proposeBlock(view types.View) {
	createStart := time.Now()
	block := r.Safety.MakeProposal(view, r.pd.GeneratePayload())
	log.Debug("block payload is ", len(block.Payload))
	r.totalBlockSize += len(block.Payload)
	r.proposedNo++
	createEnd := time.Now()
	createDuration := createEnd.Sub(createStart)
	block.Timestamp = time.Now()
	r.totalCreateDuration += createDuration
	//log.Debug("tar0", r.pm.GetCurView())
	if r.alg == "newConsensus" {
		log.Info("block send to enc ")
		r.enc.MessageCh <- *block
	} else {
		r.Broadcast(block)
	}
	_ = r.Safety.ProcessBlock(block)
	r.voteStart = time.Now()
}

// // ListenLocalEvent listens new view and timeout events
//
//	func (r *Replica) ListenLocalEvent() {
//		r.lastViewTime = time.Now()
//		r.timer = time.NewTimer(r.pm.GetTimerForView())
//		for {
//			r.timer.Reset(r.pm.GetTimerForView())
//			//L:
//			//	for {
//			//		select {
//			//		case view := <-r.pm.EnteringViewEvent():
//			//			if view >= 2 {
//			//				r.totalVoteTime += time.Now().Sub(r.voteStart)
//			//			}
//			//			// measure round time
//			//			now := time.Now()
//			//			lasts := now.Sub(r.lastViewTime)
//			//			r.totalRoundTime += lasts
//			//			r.roundNo++
//			//			r.lastViewTime = now
//			//			r.eventChan <- view
//			//			log.Debugf("[%v] the last view lasts %v milliseconds, current view: %v", r.ID(), lasts.Milliseconds(), view)
//			//			break L
//			//		case <-r.timer.C:
//			//			r.Safety.ProcessLocalTmo(r.pm.GetCurView())
//			//			break L
//			//		}
//			//	}
//			<-r.timer.C
//			now := time.Now()
//			lasts := now.Sub(r.lastViewTime)
//			r.totalRoundTime += lasts
//			r.roundNo++
//			r.lastViewTime = now
//			//log.Info("[%v] the last view lasts %v milliseconds, current view: %v", r.ID(), lasts.Milliseconds(), r.pm.GetCurView())
//			view := r.pm.GetCurView()
//			//log.Debug("tar0", view)
//			r.pm.AdvanceView(view)
//			curview := <-r.pm.EnteringViewEvent()
//			r.eventChan <- curview
//		}
//	}
//
// ListenLocalEvent listens new view and timeout events
func (r *Replica) ListenLocalEvent() {
	r.lastViewTime = time.Now()
	r.timer = time.NewTimer(r.pm.GetTimerForView())
	for {
		r.timer.Reset(r.pm.GetTimerForView())
	L:
		for {
			select {
			case view := <-r.pm.EnteringViewEvent():
				if view >= 2 {
					r.totalVoteTime += time.Now().Sub(r.voteStart)
				}
				// measure round time
				now := time.Now()
				lasts := now.Sub(r.lastViewTime)
				r.totalRoundTime += lasts
				r.roundNo++
				r.lastViewTime = now
				r.eventChan <- view
				log.Debugf("[%v] the last view lasts %v milliseconds, current view: %v", r.ID(), lasts.Milliseconds(), view)
				break L
			case <-r.timer.C:
				r.Safety.ProcessLocalTmo(r.pm.GetCurView())
				break L
			}
		}
	}
}

// ListenCommittedBlocks listens committed blocks and forked blocks from the protocols
func (r *Replica) ListenCommittedBlocks() {
	for {
		select {
		case committedBlock := <-r.committedBlocks:
			r.processCommittedBlock(committedBlock)
		case forkedBlock := <-r.forkedBlocks:
			r.processForkedBlock(forkedBlock)
		}
	}
}
func (r *Replica) ListenCoderBlock() {
	for {
		CodeBlock := <-r.enc.OutputCh
		//log.Info("try to decode coblock", CodeBlock.View)
		r.eventChan <- blockchain.DecotoBlock(*CodeBlock)
		//log.Info("listenpace", r.pm.GetCurView())
	}
}

func (r *Replica) SetStopTime() {

	timer := time.NewTimer(time.Second * time.Duration(config.Configuration.Benchmark.T))
	for {
		<-timer.C
		latency := float64(r.totalDelay) / float64(r.latencyNo)
		log.Infof("Time: %v s. Throughput: %v txs/s\n", time.Now().Sub(r.startTime).Seconds(), float64(r.totalCommittedTx)/float64(config.Configuration.Benchmark.T))
		log.Infof("Latency: %v\n", latency)
		r.endTarget = true
		r.GetChainStatus()
		return
	}
}

func (r *Replica) startSignal() {
	if !r.isStarted.Load() {
		r.startTime = time.Now()
		r.tmpTime = time.Now()
		log.Debugf("[%v] is boosting", r.ID())
		r.isStarted.Store(true)
		r.start <- true
	}
}

// Start starts event loop
func (r *Replica) Start() {
	go r.Run()
	// wait for the start signal
	<-r.start
	go r.ListenLocalEvent()
	go r.ListenCommittedBlocks()
	go r.ListenCoderBlock()
	go r.SetStopTime()
	for r.isStarted.Load() {
		if r.endTarget == true {
			return
		}
		event := <-r.eventChan
		//log.Info("type is ", reflect.TypeOf(event))
		switch v := event.(type) {
		case types.View:
			r.processNewView(v)
		case blockchain.Block:
			startProcessTime := time.Now()
			r.totalProposeDuration += startProcessTime.Sub(v.Timestamp)
			_ = r.Safety.ProcessBlock(&v)
			r.totalProcessDuration += time.Now().Sub(startProcessTime)
			r.voteStart = time.Now()
			r.processedNo++
		case blockchain.Vote:
			startProcessTime := time.Now()
			r.Safety.ProcessVote(&v)
			processingDuration := time.Now().Sub(startProcessTime)
			r.totalVoteTime += processingDuration
			r.voteNo++
		case pacemaker.TMO:
			r.Safety.ProcessRemoteTmo(&v)
		case blockchain.QC:
			r.Safety.ProcessCertificate(&v)
		}
	}
}
