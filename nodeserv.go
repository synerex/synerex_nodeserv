package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/google/gops/agent"
	nodepb "github.com/synerex/synerex_nodeapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"sort"
	"sync"
	"time"
)

//go:generate protoc -I ../nodeapi --go_out=paths=source_relative,plugins=grpc:../nodeapi ../nodeapi/nodeapi.proto

// NodeID Server for  keep all node ID
//    node ID = 0-1023. (less than 10 is for server)
// When we use sxutil, we need to support nodenum

// Function
//   register node (in the future authentication..)

// shuold use only at here

// MaxNodeNum  Max node Number
const MaxNodeNum = 1024

// MaxServerID  Max Market Server Node ID (Small number ID is for synerex server)
const MaxServerID = 10
const DefaultDuration int32 = 10 // need keepalive for each 10 sec.
const MaxDurationCount = 3       // duration count.
const defaultNodeInfoFile = "nodeinfo.json"
const defaultSxProfile = "sxprofile.json"

type eachNodeInfo struct {
	NodeName  string    `json:"name"`
	NodePBase string    `json:"nodepbase"`
	Secret    uint64    `json:"secret"`
	Address   string    `json:"address"`
	NodeType   nodepb.NodeType    `json:"nodeType"`
	ServerAddress string `json:"serverAddress"`
	ChannelTypes []uint32 `json:"channels"`
	LastAlive time.Time `json:"lastAlive"`
	Count     int32     `json:"count"`
	Status    int32     `json:"status"`
	Arg       string    `json:"arg"`
	Duration  int32     `json:"duration"`  // duration for checking next time
}

type SynerexServerInfo struct {
	NodeId       int32 	`json:"nodeid"`
	ServerInfo   string
	ChannelTypes []uint32
	ClusterId    int32
	AreaId       string
}

type SynerexGatewayInfo struct {
	NodeId int32 	`json:"nodeid"`
	ChannelTypes []uint32
	ClusterId int32
	AreaId string
}


type nodeInfo struct{
	NodeId int32	`json:"nodeid"`
	Info eachNodeInfo `json:"info"`
}

type srvNodeInfo struct {
	nodeMap map[int32]*eachNodeInfo // map from nodeID to eachNodeInfo
}

var (
	port      = flag.Int("port", 9990, "Node Server Listening Port")
	restart	  = flag.Bool("restart", false, "Restart flag: if true, load nodeinfo.json ")
	srvInfo   srvNodeInfo
	sxProfile = make([]SynerexServerInfo,0,1)
	lastNode  int32 = MaxServerID // start ID from MAX_SERVER_ID to MAX_NODE_NUM
	lastPrint time.Time
	nmmu      sync.RWMutex
)

func init() {
	log.Println("Starting Node Server..")
	rand.Seed(time.Now().UnixNano())
	s := &srvInfo
	s.nodeMap = make(map[int32]*eachNodeInfo)
	lastPrint = time.Now()
	go keepNodes(s)
}

// find unused ID from map.
// TODO: if nodeserv could be restarted, this might be problem.
// we need to save current node information to some storage
func getNextNodeID(nodeType nodepb.NodeType) int32 {
	var n int32
	if nodeType == nodepb.NodeType_SERVER {
		n = 0
	} else {
		n = lastNode
	}
	nmmu.RLock()
	for {
		_, ok := srvInfo.nodeMap[n]
		if !ok { // found empty nodeID.
			break
		}
		if nodeType == nodepb.NodeType_SERVER  {
			n = (n + 1) % MaxServerID
		} else {
			n = (n-9)%(MaxNodeNum-MaxServerID) + MaxServerID
		}
		if n == lastNode || n == 0 { // loop
			nmmu.RUnlock()
			return -1 // all id is full...
		}
	}
	nmmu.RUnlock()
	if nodeType != nodepb.NodeType_SERVER  {
		lastNode = n
	}
	return n
}

func loadSxProfile() {
	bytes, err := ioutil.ReadFile(defaultSxProfile)
	if err != nil {
		log.Println("Error on reading sxprofile.json ",err)
		return
	}
	jsonErr := json.Unmarshal(bytes, &sxProfile)
	if jsonErr != nil {
		log.Println("Can't unmarshall json ",jsonErr)
		return
	}
}
func loadNodeMap(s *srvNodeInfo){
	nmmu.Lock() // not need..
	bytes, err := ioutil.ReadFile(defaultNodeInfoFile)
	if err != nil {
		log.Println("Error on reading nodeinfo.json ",err)
		return
	}
	nodeLists := make([]nodeInfo, 0)
	jsonErr := json.Unmarshal(bytes, &nodeLists)
	if jsonErr != nil {
		log.Println("Can't unmarshall json ",jsonErr)
		return
	}
	for i, ninfo := range nodeLists {
//		log.Printf("%d: %v\n",i,ninfo)
		nodeLists[i].Info.LastAlive = time.Now()
		s.nodeMap[ninfo.NodeId] = &nodeLists[i].Info
	}
	loadSxProfile()
	nmmu.Unlock()
}

func saveSxProfile(){
	bytes , err  :=json.MarshalIndent(sxProfile, "", "  ")
	if err != nil {
		log.Printf("Cant marshal sxprofile")
	}
	err = ioutil.WriteFile(defaultSxProfile, bytes, 0666)
	if err != nil {
		log.Println("Error on writing sxprofile.json ",err)
	}
}

// saving nodemap
func saveNodeMap(s *srvNodeInfo){
	nodeLists := make([]nodeInfo, len(s.nodeMap))
//	file, err  := os.OpenFile(defaultNodeInfoFile, os.O_CREATE,  )
	nmmu.Lock()
	i :=0
	for k, nif := range s.nodeMap {
		nodeLists[i] = nodeInfo{ NodeId:k, Info:*nif}
		i++
	}
	bytes, err := 	json.MarshalIndent(nodeLists, "", "  ")
	if err != nil {
		panic(0)
	}
	ferr := ioutil.WriteFile(defaultNodeInfoFile, bytes, 0666 )
	if ferr != nil {
		log.Println("Error on writing nodeinfo.json ",ferr)
	}
	saveSxProfile()
	nmmu.Unlock()
}

// This is a monitoring loop for non keep-alive nodes.
func keepNodes(s *srvNodeInfo) {
	for {
		time.Sleep(time.Second * time.Duration(DefaultDuration))
		killNodes := make([]int32, 0)
		nmmu.Lock()
		for k, eni := range s.nodeMap {
			sub := time.Now().Sub(eni.LastAlive) / time.Second
			if sub > time.Duration(MaxDurationCount*DefaultDuration) {
				killNodes = append(killNodes, k)
			}
		}

		// remove nodes
		for _, k := range killNodes {
			delete(s.nodeMap, k)
		}
		// flush nodelist
		nmmu.Unlock()
		if len(killNodes) > 0 {
			saveNodeMap(s)
		}
	}
}

// display all node info
func (s *srvNodeInfo) listNodes() {
	nmmu.RLock()
	nk := make([]int32, len(s.nodeMap))
	i := 0
	for k := range s.nodeMap {
		nk[i] = k
		i++
	}
	sort.Slice(nk, func(i, j int) bool { return nk[i] < nk[j] })
	for i := range nk {
		eni := s.nodeMap[nk[i]]
		sub := time.Now().Sub(eni.LastAlive) / time.Second
		log.Printf("%2d[%1d]%20s %5s %14s %3d %2d:%3d %s\n", nk[i], eni.NodeType,eni.NodeName,eni.NodePBase, eni.Address, int(sub), eni.Count, eni.Status, eni.Arg)
	}
	nmmu.RUnlock()
}

// looking for Synerex Server which support channels
func getSynerexServer(chans []uint32) string{
//TODO: should implement!
// currently we just use first synerex server address
	if len(sxProfile) > 0{
		return sxProfile[0].ServerInfo
	}
	return ""
}

func (s *srvNodeInfo) RegisterNode(cx context.Context, ni *nodepb.NodeInfo) (nid *nodepb.NodeID, e error) {
	// registration
	n := getNextNodeID(ni.NodeType)
	if n == -1 { // no extra node ID...
		e = errors.New("No extra nodeID")
		return nil, e
	}

	r := rand.Uint64() // secret for this node
	pr, ok := peer.FromContext(cx)
	var ipaddr string
	if ok {
		ipaddr = pr.Addr.String()
	} else {
		ipaddr = "0.0.0.0"
	}
	eni := eachNodeInfo{
		NodeName:  ni.NodeName,
		NodePBase: ni.NodePbaseVersion,
		NodeType: ni.NodeType,
		Secret:    r,
		Address:   ipaddr,
		ServerAddress: ni.ServerAddress,
		ChannelTypes: ni.ChannelTypes,
		LastAlive: time.Now(),
		Duration:  DefaultDuration,
	}

	log.Println("Node Connection from :", ipaddr, ",", ni.NodeName)
	nmmu.Lock()
	s.nodeMap[n] = &eni
	if ni.NodeType == nodepb.NodeType_SERVER { // should register synerex_server profile.
		// check there is already that id
		existFlag := false
		for k , sx := range sxProfile {
			if sx.NodeId == n { // if there is same
				sxProfile[k].ServerInfo = ni.ServerAddress
				sxProfile[k].ChannelTypes = ni.ChannelTypes
				sxProfile[k].ClusterId = ni.ClusterId
				sxProfile[k].AreaId = ni.AreaId
				break
			}
		}
		if !existFlag { // no exist server
			sxProfile = append(sxProfile, SynerexServerInfo{
				NodeId:       n,
				ServerInfo:   ni.ServerAddress,
				ChannelTypes: ni.ChannelTypes,
				ClusterId:    ni.ClusterId,
				AreaId:       ni.AreaId,
			})
		}
	}else if ni.NodeType == nodepb.NodeType_GATEWAY { // gateway!

	}
	nmmu.Unlock()
	log.Println("------------------------------------------------------")
	s.listNodes()
//	log.Println("------------------------------------------------------")
	nid = &nodepb.NodeID{
		NodeId: n,
		Secret: r,
		ServerInfo: getSynerexServer(ni.ChannelTypes),
		KeepaliveDuration: eni.Duration,
	}
	saveNodeMap(s)
	return nid, nil
}

func (s *srvNodeInfo) QueryNode(cx context.Context, nid *nodepb.NodeID) (ni *nodepb.NodeInfo, e error) {
	n := nid.NodeId
	eni, ok := s.nodeMap[n]
	if !ok {
		fmt.Println("QueryNode: Can't find Node ID:", n)
		return nil, errors.New("unregistered NodeID")
	}
	ni = &nodepb.NodeInfo{NodeName: eni.NodeName}
	return ni, nil
}

func (s *srvNodeInfo) KeepAlive(ctx context.Context, nu *nodepb.NodeUpdate) (nr *nodepb.Response, e error) {
	nid := nu.NodeId
	r := nu.Secret
	ni, ok := s.nodeMap[nid]
	if !ok {
		// TODO: For enhance security, we need to profile the provider which connect with wrong NodeID.
		fmt.Println("Can't find node... nodeserv might be restarted or ... :", nid)
		pr, ok := peer.FromContext(ctx)
		var ipaddr string
		if ok {
			ipaddr = pr.Addr.String()
		} else {
			ipaddr = "0.0.0.0"
		}
		fmt.Println("Client from :", ipaddr)
		fmt.Println("") // debug workaround
		return &nodepb.Response{Ok: false, Command: nodepb.KeepAliveCommand_RECONNECT, Err: "Killed at Nodeserv"}, nil
	}
	if r != ni.Secret {
		e = errors.New("Secret Failed")
		return &nodepb.Response{Ok: false, Err: "Secret Failed"}, e
	}
	ni.LastAlive = time.Now()
	ni.Count = nu.UpdateCount
	ni.Status = nu.NodeStatus
	ni.Arg = nu.NodeArg

	if ni.LastAlive.Sub(lastPrint) > time.Second*time.Duration(DefaultDuration/2) {
		log.Println("---KeepAlive------------------------------------------")
		s.listNodes()
//		log.Println("------------------------------------------------------")
	}

	return &nodepb.Response{Ok: true, Command: nodepb.KeepAliveCommand_NONE, Err: ""}, nil
}

func (s *srvNodeInfo) UnRegisterNode(cx context.Context, nid *nodepb.NodeID) (nr *nodepb.Response, e error) {
	r := nid.Secret
	n := nid.NodeId
	ni, ok := s.nodeMap[n]
	if !ok {
		fmt.Printf("Can't find node... It's killed")
		return &nodepb.Response{Ok: false, Err: "Killed at Nodeserv"}, e
	}

	if r != ni.Secret { // secret failed
		e = errors.New("Secret Failed")
		log.Println("Invalid unregister")
		return &nodepb.Response{Ok: false, Err: "Secret Failed"}, e
	}

	// we need to remove Server
	if s.nodeMap[n].ServerAddress != "" { // this might be server
		for k, sx := range sxProfile {
			if sx.NodeId == n {
				sxProfile = append(sxProfile[:k],sxProfile[k+1:]...)
				break
			}
		}
	}

	log.Println("----------- Delete Node -----------", n, s.nodeMap[n].NodeName)
	nmmu.Lock()
	delete(s.nodeMap, n)
	nmmu.Unlock()
	s.listNodes()
//	log.Println("------------------------------------------------------")





	saveNodeMap(s)
	return &nodepb.Response{Ok: true, Err: ""}, nil
}

func prepareGrpcServer(opts ...grpc.ServerOption) *grpc.Server {
	nodeServer := grpc.NewServer(opts...)
	nodepb.RegisterNodeServer(nodeServer, &srvInfo)
	return nodeServer
}

func main() {
	if gerr := agent.Listen(agent.Options{}); gerr != nil {
		log.Fatal(gerr)
	}

	flag.Parse()

	// loading nodeinfo from file
	if *restart {
		log.Printf("loading nodeinfo.json..\n")
		loadNodeMap(&srvInfo)
		srvInfo.listNodes()
	}

	//	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption

	nodeServer := prepareGrpcServer(opts...)
	log.Printf("Starting Node Server: Waiting Connection at port :%d ...", *port)
	nodeServer.Serve(lis)
}
