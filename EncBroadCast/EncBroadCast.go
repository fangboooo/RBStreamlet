package EncBroadCast

import (
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/NebulousLabs/merkletree"
	"github.com/gitferry/bamboo/blockchain"
	"github.com/gitferry/bamboo/crypto"
	"github.com/gitferry/bamboo/election"
	"github.com/gitferry/bamboo/identity"
	"github.com/gitferry/bamboo/log"
	"github.com/gitferry/bamboo/node"
	"github.com/gitferry/bamboo/types"
	"github.com/klauspost/reedsolomon"
	"sort"
	"strconv"
)

type Val struct {
	Chunk
}

type Echo struct {
	Chunk
}

type Chunk struct {
	RootHash []byte
	// Proof[0] will containt the actual data.
	Proof    [][]byte
	Leaves   int
	Index    int
	View     types.View
	Sig      crypto.Signature
	Proposer identity.NodeID
	Length   int
}

type Rawdate struct {
	RootHash []byte
	Index    int
	view     types.View
}

type Chunks []Chunk

func (p Chunks) Len() int           { return len(p) }
func (p Chunks) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p Chunks) Less(i, j int) bool { return p[i].Index < p[j].Index }

type EncBroadcast struct {
	election.Election
	node.Node
	N                        int
	enc                      reedsolomon.Encoder
	RecvVals                 map[types.View]Val
	recvEchos                map[types.View]map[string]map[int]*Echo
	ParityShards, DataShards int
	echoSent                 map[string]map[int]bool
	outputDecoded            map[string]bool
	closeCh                  chan struct{}
	MessageCh                chan interface{}
	OutputCh                 chan *blockchain.NewCoderBlock
	BlockandMkHash           map[[32]byte]string
}

func NewencBroadcast(nodes node.Node, num int, elec election.Election) *EncBroadcast {
	Ebc := new(EncBroadcast)
	Ebc.Node = nodes
	Ebc.ParityShards = 2 * (num - 1) / 3
	Ebc.DataShards = num - Ebc.ParityShards
	enc, err := reedsolomon.New(Ebc.DataShards, Ebc.ParityShards)
	if err != nil {
		panic(err)
	}
	Ebc.Election = elec
	Ebc.enc = enc
	Ebc.N = num
	Ebc.RecvVals = make(map[types.View]Val)
	Ebc.recvEchos = make(map[types.View]map[string]map[int]*Echo)
	Ebc.echoSent = make(map[string]map[int]bool)
	Ebc.outputDecoded = make(map[string]bool)
	Ebc.MessageCh = make(chan interface{}, 100)
	Ebc.closeCh = make(chan struct{})
	Ebc.OutputCh = make(chan *blockchain.NewCoderBlock, 100)
	Ebc.BlockandMkHash = make(map[[32]byte]string)
	go Ebc.run()
	gob.Register(blockchain.Block{})
	gob.Register(Val{})
	gob.Register(Echo{})
	gob.Register(blockchain.NewCoderBlock{})
	return Ebc
}
func (e *EncBroadcast) run() {
	for {
		select {
		case <-e.closeCh:
			return
		case t := <-e.MessageCh:
			e.handleMessage(t)
		}
	}
}

func (e *EncBroadcast) handleMessage(t interface{}) {
	//log.Info("recivedMessage to enc type is ", reflect.TypeOf(t))
	var err error
	switch t.(type) {
	case blockchain.Block:
		e.handleInput(t.(blockchain.Block))
	case Val:
		err = e.handleVal(t.(Val))
		if err != nil {
			log.Debug(err)
		}
	case Echo:
		err = e.handleEcho(t.(Echo))
		if err != nil {
			log.Debug(err)
		}
	}
}

func (e *EncBroadcast) handleInput(blocks blockchain.Block) {
	Coderblock := blockchain.CodertoBlock(blocks)
	log.Debug("block.proposer is", blocks.Proposer)
	//log.Info("coderblock is  ", Coderblock.Sig)
	//log.Info("coderblock is  ", Coderblock.ID)
	rawDate, err := json.Marshal(Coderblock)
	//log.Info("coderblock is  ", string(rawDate))
	//log.Info("rawdate is  ", rawDate[0])
	lenght := len(rawDate)
	//log.Info("rawdate len is  ", len(rawDate))
	if err != nil {
		log.Debug("Marshal false", err)
		return
	}
	shards, err := e.makeShards(e.enc, rawDate)
	//log.Info("shards is  ", shards[0][0])
	if err != nil {
		log.Debug("makeShards false")
		return
	}

	reqs, err := e.makeValMessages(shards, blocks.View, lenght)
	//log.Info("reqs is  ", reqs[0])
	if err != nil {
		log.Debug("makeVal false")
		return
	}
	id, _ := strconv.Atoi(string(e.ID()))
	//log.Info("id1 is  ", id)
	proof := reqs[id-1]
	//if err := e.handleVal(proof); err != nil {
	//	log.Info("this is falut  ", id)
	//	log.Debug("handleVal false")
	//	return
	//}
	e.handleVal(proof)
	//log.Info("id2 is  ", id)
	for i := 0; i < e.N; i++ {
		if id == i+1 {
			continue
		}
		e.Send(identity.NodeID(strconv.Itoa(i+1)), reqs[i])
		//log.Info("send val to  ", id)
	}
	e.outputDecoded[string(proof.RootHash)] = true
}

func (e *EncBroadcast) makeShards(enc reedsolomon.Encoder, data []byte) ([][]byte, error) {
	shards, err := enc.Split(data)
	if err != nil {
		return nil, err
	}
	if err := enc.Encode(shards); err != nil {
		return nil, err
	}
	return shards, nil
}

func (e *EncBroadcast) makeValMessages(shards [][]byte, view types.View, lens int) ([]Val, error) {
	msgs := make([]Val, e.N)
	for i := 0; i < e.N; i++ {
		tree := merkletree.New(sha256.New())
		tree.SetIndex(uint64(i))
		for i := 0; i < e.N; i++ {
			tree.Push(shards[i])
		}
		root, proof, proofIndex, n := tree.Prove()
		msgs[i] = Val{
			Chunk{
				RootHash: root,
				Proof:    proof,
				Leaves:   int(n),
				Index:    int(proofIndex),
				View:     view,
				Proposer: e.ID(),
				Length:   lens,
			},
		}
		rawDate, _ := json.Marshal(Rawdate{
			RootHash: root,
			Index:    int(proofIndex),
			view:     view,
		})
		Sig, _ := crypto.PrivSign(rawDate, msgs[i].Proposer, nil)
		msgs[i].Sig = Sig
	}
	return msgs, nil
}

func (e *EncBroadcast) handleVal(val Val) error {
	if !e.IsLeader(val.Proposer, val.View) {
		return fmt.Errorf(
			"receiving proof from (%d) that is not from the proposing node ",
			val.Proposer,
		)
	}
	//log.Info("proposer is leader")
	if e.echoSent[string(val.RootHash)][val.Index] {
		return fmt.Errorf("received proof from (%d) more the once", val.Proposer)
	}
	//log.Info("received proof from (%d) the once", val.Proposer)
	if !e.validateVal(val) {
		return fmt.Errorf("received invalid proof from (%d)", val.Proposer)
	}
	//log.Info("received validated proof ")
	e.RecvVals[val.View] = val
	//this is fault way
	if _, ok := e.echoSent[string(val.RootHash)][val.Index]; !ok {
		e.echoSent[string(val.RootHash)] = make(map[int]bool)
	}
	e.echoSent[string(val.RootHash)][val.Index] = true
	//log.Info("e.echoSent[string(val.RootHash)][val.Index]", e.echoSent[string(val.RootHash)][val.Index])
	echo := Echo{val.Chunk}
	//log.Info("received validated proof ")
	for i := 0; i < e.N; i++ {
		id := identity.NodeID(strconv.Itoa(i + 1))
		//log.Info("id is", id)
		if e.ID() == id {
			continue
		}
		//log.Info("send echo to  ", id)
		e.Send(identity.NodeID(strconv.Itoa(i+1)), echo)
	}

	e.MessageCh <- echo
	//log.Info("send echo to myself")
	return nil
}

func (e *EncBroadcast) validateVal(req Val) bool {
	proof1 := merkletree.VerifyProof(
		sha256.New(),
		req.RootHash,
		req.Proof,
		uint64(req.Index),
		uint64(req.Leaves))
	//log.Info("proof1 is ", proof1)
	rawDate, _ := json.Marshal(Rawdate{
		RootHash: req.RootHash,
		Index:    req.Index,
		view:     req.View,
	})
	proof2, err := crypto.PubVerify(req.Sig, rawDate, req.Proposer)
	//log.Info("proof2 is ", proof2)
	if err != nil {
		return false
	}
	index := strconv.Itoa(req.Index + 1)
	return proof1 && proof2 && index == string(e.ID())
}

func (e *EncBroadcast) validateEcho(req Echo) bool {
	proof1 := merkletree.VerifyProof(
		sha256.New(),
		req.RootHash,
		req.Proof,
		uint64(req.Index),
		uint64(req.Leaves))
	rawDate, _ := json.Marshal(Rawdate{
		RootHash: req.RootHash,
		Index:    req.Index,
		view:     req.View,
	})
	proof2, err := crypto.PubVerify(req.Sig, rawDate, req.Proposer)
	if err != nil {
		return false
	}
	return proof1 && proof2
}

func (e *EncBroadcast) handleEcho(echo Echo) error {
	//log.Info("tar1 is ", e.outputDecoded[string(echo.RootHash)])
	//log.Info("tar1 is ", e.recvEchos[echo.View][string(echo.RootHash)][echo.Index])
	if e.outputDecoded[string(echo.RootHash)] || e.recvEchos[echo.View][string(echo.RootHash)][echo.Index] != nil {
		return fmt.Errorf(
			"receiving Echo twice",
		)
	}
	//log.Info("tar2 is ")
	if !e.IsLeader(echo.Proposer, echo.View) {
		return fmt.Errorf(
			"receiving echo from (%d) that is not from the proposing node ",
			echo.Proposer,
		)
	}
	//log.Info("tar3 is ")
	if !e.validateEcho(echo) {
		return fmt.Errorf("received invalid proof from (%d)", echo.Proposer)
	}
	//log.Info("tar4 is ")
	if _, ok := e.recvEchos[echo.View]; !ok {
		e.recvEchos[echo.View] = make(map[string]map[int]*Echo)
	}
	//log.Info("tar5 is ")
	if _, ok := e.recvEchos[echo.View][string(echo.RootHash)]; !ok {
		e.recvEchos[echo.View][string(echo.RootHash)] = make(map[int]*Echo)
	}
	//log.Info("tar6 is ")
	e.recvEchos[echo.View][string(echo.RootHash)][echo.Index] = &echo
	if !e.outputDecoded[string(echo.RootHash)] && len(e.recvEchos[echo.View][string(echo.RootHash)]) >= e.DataShards {
		//log.Info("tar7 is ")
		return e.tryDecodeValue(echo.View, echo.RootHash)
	}
	return nil
}

func (e *EncBroadcast) tryDecodeValue(view types.View, hash []byte) error {
	if e.outputDecoded[string(hash)] {
		return fmt.Errorf(
			"已经解码出了关于这个默克尔树根的内容",
			hash,
		)
	}
	//log.Info("decode tar1")
	// At this point we can decode the shards. First we create a new slice of
	// only sortable proof values.
	e.outputDecoded[string(hash)] = true
	var prfs Chunks
	for _, echo := range e.recvEchos[view][string(hash)] {
		prfs = append(prfs, echo.Chunk)
	}
	sort.Sort(prfs)
	//log.Info("decode tar2")

	// Reconstruct the value with reedsolomon encoding.
	shards := make([][]byte, e.ParityShards+e.DataShards)
	for _, p := range prfs {
		shards[p.Index] = p.Proof[0]
	}
	if err := e.enc.Reconstruct(shards); err != nil {
		return fmt.Errorf(
			"解码失败1",
		)
	}
	var value []byte
	for _, data := range shards[:e.DataShards] {
		value = append(value, data...)
	}
	//log.Info("decode tar3")
	var Coblock blockchain.NewCoderBlock
	//log.Info("decode tar4")
	//log.Info("coderblock is  ", string(value))
	//lenght := prfs[0].Length
	//log.Info("date len is  ", len(value))
	lens := prfs[0].Length
	err := json.Unmarshal(value[:lens], &Coblock)
	//err := rlp.Decode(bytes.NewBuffer(value), &Coblock)
	//log.Info("try to decode coblock", Coblock.View)
	//log.Info("try to decode coblock", reflect.TypeOf(Coblock))
	//log.Info("try to decode coblock1 ", Coblock.View)
	if err != nil {
		log.Info(err)
		return fmt.Errorf(
			"解码失败2",
		)
	}
	//block := blockchain.DecotoBlock(Coblock)
	delete(e.recvEchos[types.View(Coblock.View)], string(hash))
	//log.Info("try to decode coblock2 ", Coblock.View)
	e.OutputCh <- &Coblock
	e.BlockandMkHash[Coblock.ID] = string(hash)
	//log.Info("decode right ")
	//log.Info("coderblock is  ", Coblock.Sig)
	return nil
}
