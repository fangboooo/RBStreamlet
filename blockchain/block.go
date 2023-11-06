package blockchain

import (
	"strconv"
	"time"

	"github.com/gitferry/bamboo/crypto"
	"github.com/gitferry/bamboo/identity"
	"github.com/gitferry/bamboo/message"
	"github.com/gitferry/bamboo/types"
)

type CoderBlock struct {
	types.View
	QC        QC
	Proposer  identity.NodeID
	Timestamp time.Time
	Payload   []message.CodeTransaction
	PrevID    [32]byte
	Sig       [][]byte
	ID        [32]byte
	Ts        time.Duration
}

type NewCoderBlock struct {
	View      int
	QC        NewQC
	Proposer  int
	Timestamp time.Time
	Payload   []message.CodeTransaction
	PrevID    [32]byte
	Sig       [][]byte
	ID        [32]byte
	Ts        time.Duration
}

func CodertoBlock(block Block) NewCoderBlock {
	var coderBlock NewCoderBlock
	coderBlock.View = int(block.View)
	coderBlock.QC.Leader = string(block.QC.Leader)
	coderBlock.QC.BlockID = block.QC.BlockID
	coderBlock.QC.View = int(block.QC.View)
	for _, txn := range block.Payload {
		coderBlock.Payload = append(coderBlock.Payload, message.CodeTransaction{
			txn.Command,
			txn.Properties,
			txn.Timestamp,
			(txn.NodeID).Node(),
			txn.ID,
		})
	}
	coderBlock.Proposer = block.Proposer.Node()
	coderBlock.Timestamp = block.Timestamp
	coderBlock.PrevID = block.PrevID
	coderBlock.Sig = block.Sig
	coderBlock.ID = block.ID
	coderBlock.Ts = block.Ts
	return coderBlock
}

func DecotoBlock(Coblock NewCoderBlock) Block {
	var block Block
	//log.Info("tar1")
	block.View = types.View(Coblock.View)
	//log.Info("tar2", Coblock.Proposer)
	block.Proposer = identity.NodeID(strconv.Itoa(Coblock.Proposer))
	//log.Info("tar2", block.View)
	block.QC = &QC{}
	//log.Info("tar2", Coblock.QC.BlockID)
	block.QC.BlockID = crypto.Identifier(Coblock.QC.BlockID)
	//log.Info("tar3", block.QC.BlockID)
	//log.Info("tar4", block.QC.Leader)
	block.QC.Leader = identity.NodeID(Coblock.QC.Leader)
	//log.Info("tar4", block.QC.Leader)
	block.QC.View = types.View(Coblock.QC.View)
	block.QC.Signers = nil
	block.QC.Signature = nil
	block.QC.AggSig = nil
	//log.Info("tar3", block.QC)
	for _, txn := range Coblock.Payload {
		block.Payload = append(block.Payload, &message.Transaction{
			Command:    txn.Command,
			Properties: txn.Properties,
			Timestamp:  txn.Timestamp,
			NodeID:     identity.NewNodeID(txn.NodeID),
			ID:         txn.ID,
			C:          make(chan message.TransactionReply),
		})
	}
	//log.Info("tar3", block.Payload)
	block.Timestamp = Coblock.Timestamp
	//log.Info("tar4", block.Timestamp)
	block.PrevID = Coblock.PrevID
	//log.Info("tar5", block.PrevID)
	block.Sig = Coblock.Sig
	//log.Info("tar6", block.Sig)
	block.ID = Coblock.ID
	//log.Info("tar7", block.ID)
	block.Ts = Coblock.Ts
	//log.Info("tar8", block.Ts)
	return block
}

type CommitMessage struct {
	View     types.View
	Proposer identity.NodeID
}

type CommitMessages []*CommitMessage

func (c CommitMessages) Len() int           { return len(c) }
func (c CommitMessages) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c CommitMessages) Less(i, j int) bool { return c[i].View < c[j].View }

type Block struct {
	types.View
	QC        *QC
	Proposer  identity.NodeID
	Timestamp time.Time
	Payload   []*message.Transaction
	PrevID    crypto.Identifier
	Sig       crypto.Signature
	ID        crypto.Identifier
	Ts        time.Duration
}

type rawBlock struct {
	types.View
	QC       *QC
	Proposer identity.NodeID
	Payload  []string
	PrevID   crypto.Identifier
	Sig      crypto.Signature
	ID       crypto.Identifier
}

// MakeBlock creates an unsigned block
func MakeBlock(view types.View, qc *QC, prevID crypto.Identifier, payload []*message.Transaction, proposer identity.NodeID) *Block {
	b := new(Block)
	b.View = view
	b.Proposer = proposer
	b.QC = qc
	b.Payload = payload
	b.PrevID = prevID
	b.makeID(proposer)
	return b
}

func (b *Block) makeID(nodeID identity.NodeID) {
	raw := &rawBlock{
		View:     b.View,
		QC:       b.QC,
		Proposer: b.Proposer,
		PrevID:   b.PrevID,
	}
	var payloadIDs []string
	for _, txn := range b.Payload {
		payloadIDs = append(payloadIDs, txn.ID)
	}
	raw.Payload = payloadIDs
	b.ID = crypto.MakeID(raw)
	// TODO: uncomment the following
	b.Sig, _ = crypto.PrivSign(crypto.IDToByte(b.ID), nodeID, nil)
}

//func (b *Block) fakemakeID(nodeID identity.NodeID) {
//	raw := &rawBlock{
//		View:     b.View,
//		QC:       b.QC,
//		Proposer: b.Proposer,
//		PrevID:   b.PrevID,
//	}
//	var payloadIDs []string
//	for _, txn := range b.Payload {
//		payloadIDs = append(payloadIDs, txn.ID)
//	}
//	raw.Payload = payloadIDs
//	b.ID = crypto.MakeID(raw)
//	// TODO: uncomment the following
//	b.Sig = [][]byte{{1, 2}, {2, 3}}
//}
