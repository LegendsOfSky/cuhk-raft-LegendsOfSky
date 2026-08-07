package main

import (
	"context"
	"cuhk/asgn/raft"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
)

func main() {
	ports := os.Args[2]
	myPort, _ := strconv.Atoi(os.Args[1])
	nodeId, _ := strconv.Atoi(os.Args[3])
	heartBeatInterval, _ := strconv.Atoi(os.Args[4])
	electionTimeout, _ := strconv.Atoi(os.Args[5])

	portStrings := strings.Split(ports, ",")

	// A map where
	// 		the key is the node id
	//		the value is the {hostname:port}
	nodeIdPortMap := make(map[int]int)
	for i, portStr := range portStrings {
		port, _ := strconv.Atoi(portStr)
		nodeIdPortMap[i] = port
	}

	// Create and start the Raft Node.
	_, err := NewRaftNode(myPort, nodeIdPortMap,
		nodeId, heartBeatInterval, electionTimeout)

	if err != nil {
		log.Fatalln("Failed to create raft node:", err)
	}

	// Run the raft node forever.
	select {}
}

type raftNode struct {
	// global info
	leaderId       int
	totalNodeCount int

	// node info
	nodeId            int
	nodeRole          raft.Role
	hostConnectionMap map[int]raft.RaftNodeClient

	// notification channel
	electionIntervalChangedSignalChannel  chan bool
	heartBeatIntervalChangedSignalChannel chan bool
	heartBeatReceivedSignalChannel        chan bool
	revertToFollowerSignalChannel         chan bool

	// persistent state on all servers (Should be updated before repsonding to RPCs)
	currentTerm int
	votedFor    int
	votingTerm  int
	log         []*raft.LogEntry
	logCount    int

	// volatile state on all servers
	commitIndex              int
	electionTimeoutInterval  int
	heartBeatTimeoutInterval int

	// volatile state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

// NewRaftNode Summary
//
// creates a new RaftNode.
//
// # Parameters
//
// myPort: the port of this new node. We use tcp in this project.
// (Note: Please listen to this port rather than nodeidPortMap[nodeId])
//
// nodeIdPortMap: a map from all node IDs to their ports.
//
// nodeId: the id of this node
//
// heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
//
// electionTimeout: The election timeout for this node. In millisecond.
//
// # Returns
//
// This function should return only when all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes.
func NewRaftNode(
	myPort int, nodeIdPortMap map[int]int, nodeId, heartBeatInterval, electionTimeout int) (
	raft.RaftNodeServer, error) {
	// TODO: Implement this!

	delete(nodeIdPortMap, nodeId) // remove myself in the host-map

	hostConnectionMap := make(map[int]raft.RaftNodeClient) // a map for {node id, gRPCClient}

	newRaftNode := raftNode{
		// global info
		totalNodeCount: 0,
		leaderId:       -1,

		// node info
		nodeId:            nodeId,
		nodeRole:          raft.Role_Follower,
		hostConnectionMap: hostConnectionMap,

		// notification channel
		electionIntervalChangedSignalChannel:  make(chan bool),
		heartBeatIntervalChangedSignalChannel: make(chan bool),
		heartBeatReceivedSignalChannel:        make(chan bool),
		revertToFollowerSignalChannel:         make(chan bool),

		// persistent state on all servers
		currentTerm: 0,
		votedFor:    -1,
		votingTerm:  -1,
		log:         make([]*raft.LogEntry, 10000),
		logCount:    0,

		// volatile state on all servers
		commitIndex:              0,
		electionTimeoutInterval:  electionTimeout,
		heartBeatTimeoutInterval: heartBeatInterval,

		// volatile state on leaders
		nextIndex:  nil,
		matchIndex: nil,
	}
	initialLog := raft.LogEntry{Term: 0}
	newRaftNode.log[0] = &initialLog
	newRaftNode.logCount++

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", myPort))
	if err != nil {
		log.Println("Fail to listen port", err)
		os.Exit(1)
	}

	server := grpc.NewServer()
	raft.RegisterRaftNodeServer(server, &newRaftNode)

	log.Printf("Start listening to port: %d\n", myPort)
	go func() {
		err := server.Serve(listener)
		if err != nil {
			log.Printf("Server listening to port %d failed.\n", myPort)
		}
	}()

	// Try connecting nodes
	for hostId, hostPorts := range nodeIdPortMap {
		currentHostId := int(hostId)
		numTry := 0
		for {
			numTry++

			conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", hostPorts), grpc.WithInsecure(), grpc.WithBlock())
			// defer conn.Close()
			client := raft.NewRaftNodeClient(conn)
			if err != nil {
				log.Println("Fail to connect other nodes. ", err)
				time.Sleep(1 * time.Second)
			} else {
				hostConnectionMap[currentHostId] = client
				break
			}
		}
	}
	log.Printf("Successfully connect all nodes.")

	/* finish remaining initialization of the new raft node */
	newRaftNode.totalNodeCount = len(hostConnectionMap) + 1

	//TODO: kick off leader election here !
	go func() {
		for {
			switch newRaftNode.nodeRole {
			case raft.Role_Follower:
				newRaftNode.runAsFollower()
			case raft.Role_Candidate:
				newRaftNode.runAsCandidate()
			case raft.Role_Leader:
				newRaftNode.runAsLeader()
			}
		}
	}()
	return &newRaftNode, nil
}

// Propose Summary
//
// initializes proposing a new operation, and replies with the result of committing this operation. Propose
// should not return until this operation has been committed, or this node is not leader now.
//
// If we put a new <k, v> pair or deleted an existing <k, v> pair successfully, it should return OK;
//
// If it tries to delete a non-existing key, a KeyNotFound should be returned;
//
// If this node is not leader now, it should return WrongNode as well as the currentLeader id.
func (rn *raftNode) Propose(ctx context.Context, args *raft.ProposeArgs) (*raft.ProposeReply, error) {
	routePrefix := computeRoutePrefix(rn.nodeId, -1, rn.nodeId, rn.totalNodeCount)
	loggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "Propose")
	log.Printf("%s: BEGIN", loggingPrefix)

	// TODO: Implement this!
	var ret raft.ProposeReply
	return &ret, nil
}

// GetValue Summary
//
// looks up the value for a key, and replies with the value or with the Status KeyNotFound.
func (rn *raftNode) GetValue(ctx context.Context, args *raft.GetValueArgs) (*raft.GetValueReply, error) {
	routePrefix := computeRoutePrefix(rn.nodeId, -1, rn.nodeId, rn.totalNodeCount)
	loggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "GetValue")
	log.Printf("%s: BEGIN", loggingPrefix)

	// TODO: Implement this!
	var ret raft.GetValueReply
	return &ret, nil
}

// RequestVote Summary
//
// Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
//
// # Parameters
//
// voteRequestArgs: The RequestVote Message. From (src node id) and To (dst node id) must be included.
//
// TODO: rename to HandleVoteRequest
func (rn *raftNode) RequestVote(ctx context.Context, args *raft.RequestVoteArgs) (
	*raft.RequestVoteReply, error) {
	routePrefix := computeRoutePrefix(rn.nodeId, int(args.From), rn.nodeId, rn.totalNodeCount)
	loggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "RequestVote")
	log.Printf("%s: BEGIN", loggingPrefix)
	log.Printf("%s: Handle vote request from node %d.", loggingPrefix, args.From)

	reply := raft.RequestVoteReply{
		From:        int32(rn.nodeId),
		To:          args.From,
		Term:        int32(rn.currentTerm),
		VoteGranted: false,
	}

	/* reject when requester has lower term */
	if int(args.Term) < rn.currentTerm {
		log.Printf("%s: Requester has lower term (%d), vote request rejected.", loggingPrefix, args.Term)
		return &reply, nil
	}

	if int(args.Term) > rn.currentTerm {
		log.Printf("%s: Requester has higher term (%d).", loggingPrefix, args.Term)
		rn.currentTerm = int(args.Term)
		reply.Term = int32(rn.currentTerm)

		if rn.nodeRole == raft.Role_Follower {
			log.Printf("%s: Heart beat signal received.", loggingPrefix)
			rn.heartBeatReceivedSignalChannel <- true
		} else if rn.nodeRole == raft.Role_Candidate {
			log.Printf("%s: Higher term found, revert to follower", loggingPrefix)
			rn.revertToFollowerSignalChannel <- true
		} else if rn.nodeRole == raft.Role_Leader {
			log.Printf("%s: Higher term found, revert to follower", loggingPrefix)
			rn.revertToFollowerSignalChannel <- true
			rn.leaderId = -1
		} else {
			log.Printf("%s: Invalid state (Code navigation key: RW98WuDp0eXFZZGR)", loggingPrefix)
		}
	}

	/* reject when this node has vote for others */
	if rn.votingTerm == rn.currentTerm && rn.votedFor != -1 && rn.votedFor != int(args.From) {
		log.Printf("%s: This node has voted for other node (%d), vote request rejected.", loggingPrefix, rn.votedFor)
		return &reply, nil
	}

	if rn.commitIndex <= int(args.LastLogIndex) && rn.log[rn.commitIndex].Term <= args.LastLogTerm {
		log.Printf("%s: Vote granted because log of requester is as update as this node.", loggingPrefix)
		reply.VoteGranted = true
		rn.votedFor = int(args.From)
		rn.votingTerm = rn.currentTerm
		return &reply, nil
	}

	log.Printf(
		"%s: By default, reject vote request (thisCommitIndex %d, otherCommitIndex %d, thisLastTerm %d, otherLastTerm %d).",
		loggingPrefix,
		rn.commitIndex, args.LastLogIndex, rn.log[rn.commitIndex].Term, args.LastLogTerm,
	)
	return &reply, nil
}

// AppendEntries Summary
//
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// # Parameters
//
// appendEntriesArgs: The AppendEntries Message. From (src node id) and To (dst node id) must be included.
func (rn *raftNode) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs) (
	*raft.AppendEntriesReply, error) {
	routePrefix := computeRoutePrefix(rn.nodeId, int(args.From), rn.nodeId, rn.totalNodeCount)
	loggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "AppendEntries")
	log.Printf("%s: BEGIN", loggingPrefix)
	log.Printf("%s: Append entries from node %d.", loggingPrefix, args.From)

	reply := raft.AppendEntriesReply{
		From: int32(rn.nodeId),
		To:   args.From,
		Term: int32(rn.currentTerm),

		Success:    false,
		MatchIndex: int32(rn.commitIndex),
	}

	if int(args.Term) < rn.currentTerm {
		log.Printf("%s: Append entries request REJECTED because requester has an outdated term (Impl Ref #1).", loggingPrefix)
		return &reply, nil
	}

	log.Printf("%s: Update leader to node %d", loggingPrefix, args.From)
	rn.leaderId = int(args.From)

	if rn.nodeRole == raft.Role_Follower {
		log.Printf("%s: Heart beat signal received", loggingPrefix)
		rn.heartBeatReceivedSignalChannel <- true
	} else if rn.nodeRole == raft.Role_Candidate {
		log.Printf("%s: Revert to follower (Candidate -> Follower).", loggingPrefix)
		rn.revertToFollowerSignalChannel <- true
	} else if rn.nodeRole == raft.Role_Leader {
		log.Printf("%s: TODO finish this state (Code navigation key: lm2leockDs3uGiHJ).", loggingPrefix)
	} else {
		log.Printf("%s: Error, incorrect state (Code navigation key: uKP8WWdVLb2ZlWqN).", loggingPrefix)
	}

	log.Printf("%s: Updating terms to %d.", loggingPrefix, args.Term)
	rn.currentTerm = int(args.Term)
	reply.Term = int32(rn.currentTerm)
	loggingPrefix = fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "AppendEntries")

	if rn.logCount <= int(args.PrevLogIndex) {
		log.Printf("%s: Append entries FAILED because some logs are missing prior new entries (Impl Ref #2).", loggingPrefix)
		return &reply, nil
	}

	if rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		log.Printf("%s: Remove conflicting logs and subsequent logs (Impl Ref #3).", loggingPrefix)
		rn.logCount = int(args.PrevLogIndex) + 1
		return &reply, nil
	}

	log.Printf("%s: Append new entries (Impl Ref #4).", loggingPrefix)
	for i := 0; i < len(args.Entries); i++ {
		log.Printf("%s: Append entry %d to %d.", loggingPrefix, i, rn.logCount)
		rn.log[rn.logCount] = args.Entries[i]
		rn.logCount++
	}

	log.Printf("%s: Update commit index (Impl Ref #5).", loggingPrefix)
	if int(args.LeaderCommit) > rn.commitIndex {
		rn.commitIndex = rn.logCount - 1
		if rn.commitIndex > int(args.LeaderCommit) {
			rn.commitIndex = int(args.LeaderCommit)
		}
	}

	log.Printf("%s: Append entries success.", loggingPrefix)
	reply.Success = true
	return &reply, nil
}

// SetElectionTimeout Summary
//
// Set electionTimeOut with args.Timeout in milliseconds (will stop and reset the timer).
//
// # Returns
//
// Not in use.
func (rn *raftNode) SetElectionTimeout(
	ctx context.Context, args *raft.SetElectionTimeoutArgs) (*raft.SetElectionTimeoutReply, error) {
	routePrefix := computeRoutePrefix(rn.nodeId, -1, rn.nodeId, rn.totalNodeCount)
	loggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "SetElectionTimeout")
	log.Printf("%s: BEGIN (interval = %d)", loggingPrefix, args.Timeout)

	rn.electionTimeoutInterval = int(args.Timeout)
	rn.electionIntervalChangedSignalChannel <- true

	reply := raft.SetElectionTimeoutReply{}
	return &reply, nil
}

// SetHeartBeatInterval Summary
//
// Set heartBeatInterval with args.Interval in milliseconds (will stop and reset the timer).
//
// # Returns
//
// Not in use.
func (rn *raftNode) SetHeartBeatInterval(ctx context.Context, args *raft.SetHeartBeatIntervalArgs) (
	*raft.SetHeartBeatIntervalReply, error) {
	routePrefix := computeRoutePrefix(rn.nodeId, -1, rn.nodeId, rn.totalNodeCount)
	loggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "SetHeartBeatInterval")
	log.Printf("%s: BEGIN (interval = %d)", loggingPrefix, args.Interval)

	rn.heartBeatTimeoutInterval = int(args.Interval)
	rn.heartBeatIntervalChangedSignalChannel <- true

	reply := raft.SetHeartBeatIntervalReply{}
	return &reply, nil
}

// CheckEvents WARNING
//
// NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	return nil, nil
}

func (rn *raftNode) runAsFollower() {
	routePrefix := computeRoutePrefix(rn.nodeId, -1, rn.nodeId, rn.totalNodeCount)
	loggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "runAsFollower")
	log.Printf("%s: BEGIN", loggingPrefix)

	electionTimerTimeoutTriggerChannel := time.After(time.Duration(rn.electionTimeoutInterval) * time.Millisecond)
	select {
	case <-rn.heartBeatReceivedSignalChannel:
		log.Printf("%s: Heart beat received, restart election timeout timer.", loggingPrefix)
		return

	case <-electionTimerTimeoutTriggerChannel:
		log.Printf("%s: Election timeout -> becoming candidate.", loggingPrefix)
		rn.nodeRole = raft.Role_Candidate
		return

	case <-rn.electionIntervalChangedSignalChannel:
		log.Printf("%s: Election timer reset. Restart as follower. (temporary logic, may need fix).", loggingPrefix)
		return
	}
}

func (rn *raftNode) runAsCandidate() {
	routePrefix := computeRoutePrefix(rn.nodeId, -1, rn.nodeId, rn.totalNodeCount)
	loggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "runAsCandidate")
	log.Printf("%s: BEGIN", loggingPrefix)

	rn.currentTerm++
	log.Printf("%s: advancing to term %d.", loggingPrefix, rn.currentTerm)
	loggingPrefix = fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "runAsCandidate")
	rn.votedFor = rn.nodeId // vote for self
	rn.votingTerm = rn.currentTerm

	voteReplyChannel := make(chan *raft.RequestVoteReply, len(rn.hostConnectionMap))
	rn.sendVoteRequestToOtherNodes(voteReplyChannel)

	voteGranted := 1
	voteReceived := 1
	for {
		log.Printf("%s: start waiting for vote replies until %d of election timer runs out.", loggingPrefix, rn.electionTimeoutInterval)
		electionTimerTimeoutTriggerChannel := time.After(time.Duration(rn.electionTimeoutInterval) * time.Millisecond)
	ListeningLoop:
		for {
			select {
			case <-rn.revertToFollowerSignalChannel:
				log.Printf("%s: Revert to follower.", loggingPrefix)
				rn.nodeRole = raft.Role_Follower
				return

			case <-rn.electionIntervalChangedSignalChannel:
				log.Printf("%s: Election timer reset.", loggingPrefix)
				break ListeningLoop

			case <-electionTimerTimeoutTriggerChannel:
				log.Printf("%s: Election timeout, restart election.", loggingPrefix)
				return

			case voteReply := <-voteReplyChannel:
				replyRoutePrefix := computeRoutePrefix(int(voteReply.From), int(voteReply.To), rn.nodeId, rn.totalNodeCount)
				replyLoggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", replyRoutePrefix, rn.nodeId, rn.currentTerm, "runAsCandidate")

				if int(voteReply.Term) > rn.currentTerm {
					log.Printf("%s: Found higher term, revert to follower role.", replyLoggingPrefix)
					rn.currentTerm = int(voteReply.Term)
					rn.nodeRole = raft.Role_Follower
					return
				}

				log.Printf("%s: Processing vote reply.", replyLoggingPrefix)
				if int(voteReply.Term) < rn.currentTerm {
					log.Printf("%s: Ignore for votes from previous term %d.", replyLoggingPrefix, voteReply.Term)
				} else {
					voteReceived++
					if !voteReply.VoteGranted {
						log.Printf("%s: Being rejected from node %d.", replyLoggingPrefix, voteReply.From)
					} else {
						voteGranted++
						log.Printf(
							"%s: Vote granted from node %d (%d/%d/%d).",
							replyLoggingPrefix, voteReply.From, voteGranted, voteReceived, rn.totalNodeCount,
						)

						if voteGranted >= (rn.totalNodeCount/2 + 1) {
							log.Printf("%s: Enough votes received.", loggingPrefix)
							rn.nodeRole = raft.Role_Leader
							rn.leaderId = rn.nodeId

							log.Printf("%s: Becoming leader.", loggingPrefix)
							rn.initializeLeaderRequiredField()

							return
						}
					}
				}
			}
		}
	}
}

func (rn *raftNode) runAsLeader() {
	routePrefix := computeRoutePrefix(rn.nodeId, -1, rn.nodeId, rn.totalNodeCount)
	loggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "runAsLeader")
	log.Printf("%s: BEGIN", loggingPrefix)

	/* send heart beat to other nodes */
	appendEntriesReplyChannel := make(chan *raft.AppendEntriesReply, len(rn.hostConnectionMap))
	for clientNodeId, client := range rn.hostConnectionMap {
		go rn.appendEntriesToOtherNode(clientNodeId, client, appendEntriesReplyChannel)
	}

	heartBeatTimerTimeoutTriggerChannel := time.After(time.Duration(rn.heartBeatTimeoutInterval) * time.Millisecond)
	for {
		select {
		case <-rn.revertToFollowerSignalChannel:
			log.Printf("%s: Revert to follower.", loggingPrefix)
			rn.nodeRole = raft.Role_Follower
			rn.leaderId = -1
			return

		case <-heartBeatTimerTimeoutTriggerChannel:
			log.Printf("%s: Heart beat timer triggered.", loggingPrefix)
			return

			// TODO handle command sent client
		}
	}
}

func (rn *raftNode) initializeLeaderRequiredField() {
	routePrefix := computeRoutePrefix(rn.nodeId, -1, rn.nodeId, rn.totalNodeCount)
	loggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "initLeaderReqField")
	log.Printf("%s: BEGIN", loggingPrefix)
	log.Printf("%s: Initialize leader required field.", loggingPrefix)

	rn.nextIndex = make(map[int]int, len(rn.hostConnectionMap))
	rn.matchIndex = make(map[int]int, len(rn.hostConnectionMap))
	for clientNodeId := range rn.hostConnectionMap {
		rn.nextIndex[clientNodeId] = rn.logCount
		rn.matchIndex[clientNodeId] = 0
	}
}

func (rn *raftNode) sendVoteRequestToOtherNodes(voteReplyChannel chan *raft.RequestVoteReply) {
	routePrefix := computeRoutePrefix(rn.nodeId, -1, rn.nodeId, rn.totalNodeCount)
	loggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "sendVoteRequests")
	log.Printf("%s: BEGIN", loggingPrefix)
	log.Printf("%s: Sending vote request to other nodes.", loggingPrefix)

	for clientNodeId, client := range rn.hostConnectionMap {
		sendRoutePrefix := computeRoutePrefix(rn.nodeId, clientNodeId, rn.nodeId, rn.totalNodeCount)
		sendLoggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", sendRoutePrefix, rn.nodeId, rn.currentTerm, "sendVoteRequests")

		/* send vote request to other raft node */
		go func(clientNodeId int, client raft.RaftNodeClient) {
			log.Printf("%s: Sending RequestVote() RPC to node %d.", sendLoggingPrefix, clientNodeId)

			args := raft.RequestVoteArgs{
				From: int32(rn.nodeId),
				To:   int32(clientNodeId),
				Term: int32(rn.currentTerm),

				CandidateId:  int32(rn.nodeId),
				LastLogIndex: int32(rn.commitIndex),
				LastLogTerm:  rn.log[rn.commitIndex].Term,
			}
			reply, err := client.RequestVote(context.Background(), &args)
			if err != nil {
				log.Printf(
					"%s: Sending RequestVote() RPC to %d failed: %v (at sendVoteRequestToOtherNodes()).",
					sendLoggingPrefix, clientNodeId, err,
				)
				return
			}
			voteReplyChannel <- reply
		}(clientNodeId, client)
	}
}

func (rn *raftNode) appendEntriesToOtherNode(
	clientNodeId int, client raft.RaftNodeClient, appendEntriesReplyChannel chan *raft.AppendEntriesReply,
) {
	routePrefix := computeRoutePrefix(rn.nodeId, -1, rn.nodeId, rn.totalNodeCount)
	loggingPrefix := fmt.Sprintf("<%s Node %d Term %d %-20s      >", routePrefix, rn.nodeId, rn.currentTerm, "sendEntries")
	log.Printf("%s: BEGIN", loggingPrefix)
	log.Printf("%s: Sending AppendEntries() RPC to node %d.", loggingPrefix, clientNodeId)

	entriesCount := 0
	prevLogIndex := rn.logCount - 1
	args := raft.AppendEntriesArgs{
		From: int32(rn.nodeId),
		To:   int32(clientNodeId),
		Term: int32(rn.currentTerm),

		LeaderId:     int32(rn.nodeId),
		PrevLogIndex: int32(prevLogIndex),
		PrevLogTerm:  rn.log[prevLogIndex].Term,

		// TODO add entries
		Entries:      make([]*raft.LogEntry, entriesCount),
		LeaderCommit: int32(rn.commitIndex),
	}

	log.Printf("%s: TODO include entries", loggingPrefix)
	for i := 0; i < entriesCount; i++ {
		args.Entries[i] = &raft.LogEntry{}
	}

	reply, err := client.AppendEntries(context.Background(), &args)
	if err != nil {
		log.Printf(
			"%s: Sending AppendEntries() RPC to %d failed: %v (at appendEntriesToOtherNode()).",
			loggingPrefix, clientNodeId, err,
		)
		return
	}
	appendEntriesReplyChannel <- reply
}

func computeRoutePrefix(sourceNodeId int, targetNodeId int, loggingNodeId int, nodeCount int) string {
	maxNodeId := sourceNodeId
	if maxNodeId < targetNodeId {
		maxNodeId = targetNodeId
	}
	minNodeId := sourceNodeId
	if minNodeId > targetNodeId {
		minNodeId = targetNodeId
	}

	result := ""
	nodeIndex := nodeCount
	for nodeIndex > 0 {
		nodeIndex--

		if nodeIndex == sourceNodeId {
			if targetNodeId != -1 && sourceNodeId < targetNodeId {
				if sourceNodeId+1 == targetNodeId {
					if nodeIndex == loggingNodeId {
						result = "X>" + result
					} else {
						result = "O>" + result
					}
				} else {
					if nodeIndex == loggingNodeId {
						result = "X-" + result
					} else {
						result = "O-" + result
					}
				}
			} else {
				if nodeIndex == loggingNodeId {
					result = "X " + result
				} else {
					result = "O " + result
				}
			}
		} else if nodeIndex == targetNodeId {
			if targetNodeId < sourceNodeId {
				if nodeIndex == loggingNodeId {
					result = "X<" + result
				} else {
					result = "O<" + result
				}
			} else {
				if nodeIndex == loggingNodeId {
					result = "X " + result
				} else {
					result = "O " + result
				}
			}
		} else if targetNodeId != -1 && minNodeId < nodeIndex && nodeIndex < maxNodeId {
			if nodeIndex == targetNodeId-1 && sourceNodeId < targetNodeId {
				result = "->" + result
			} else {
				result = "--" + result
			}
		} else {
			result = "  " + result
		}
	}
	return result
}
