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
	log []*raft.LogEntry
	// TODO: Implement this!

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

	hostConnectionMap := make(map[int32]raft.RaftNodeClient) // a map for {node id, gRPCClient}

	newRaftNode := raftNode{
		log: nil,
	}

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
		currentHostId := int32(hostId)
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
	log.Printf("Successfully connect all nodes")

	//TODO: kick off leader election here !

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
func (rn *raftNode) Propose(ctx context.Context, operationToBeProposed *raft.ProposeArgs) (*raft.ProposeReply, error) {
	// TODO: Implement this!
	log.Printf("Receive propose from client")
	var ret raft.ProposeReply

	return &ret, nil
}

// GetValue Summary
//
// looks up the value for a key, and replies with the value or with the Status KeyNotFound.
func (rn *raftNode) GetValue(ctx context.Context, keyOfTheValue *raft.GetValueArgs) (*raft.GetValueReply, error) {
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
func (rn *raftNode) RequestVote(ctx context.Context, voteRequestArgs *raft.RequestVoteArgs) (
	*raft.RequestVoteReply, error) {
	// TODO: Implement this!
	var reply raft.RequestVoteReply
	return &reply, nil
}

// AppendEntries Summary
//
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// # Parameters
//
// appendEntriesArgs: The AppendEntries Message. From (src node id) and To (dst node id) must be included.
func (rn *raftNode) AppendEntries(ctx context.Context, appendEntriesArgs *raft.AppendEntriesArgs) (
	*raft.AppendEntriesReply, error) {
	// TODO: Implement this
	var reply raft.AppendEntriesReply
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
	ctx context.Context, electionTimeoutInterval *raft.SetElectionTimeoutArgs) (*raft.SetElectionTimeoutReply, error) {
	// TODO: Implement this!
	var reply raft.SetElectionTimeoutReply
	return &reply, nil
}

// SetHeartBeatInterval Summary
//
// Set heartBeatInterval with args.Interval in milliseconds (will stop and reset the timer).
//
// # Returns
//
// Not in use.
func (rn *raftNode) SetHeartBeatInterval(ctx context.Context, hearBeatInterval *raft.SetHeartBeatIntervalArgs) (
	*raft.SetHeartBeatIntervalReply, error) {
	// TODO: Implement this!
	var reply raft.SetHeartBeatIntervalReply
	return &reply, nil
}

// CheckEvents WARNING
//
// NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	return nil, nil
}
