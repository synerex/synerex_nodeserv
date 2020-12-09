package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/google/gops/agent"

	nodepb "github.com/synerex/synerex_nodeapi"
	nodecapi "github.com/synerex/synerex_nodeserv_controlapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
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
	NodeName       string          `json:"name"`
	NodePBase      string          `json:"nodepbase"`
	NodeBinVersion string          `json:"nodebinver"`
	Secret         uint64          `json:"secret"`
	Address        string          `json:"address"`
	NodeType       nodepb.NodeType `json:"nodeType"`
	ServerInfo     string          `json:"serverInfo"`
	ChannelTypes   []uint32        `json:"channels"`
	LastAlive      time.Time       `json:"lastAlive"`
	Count          int32           `json:"count"`
	Status         int32           `json:"status"`
	Arg            string          `json:"arg"`
	Duration       int32           `json:"duration"` // duration for checking next time
}

type SynerexServerInfo struct {
	NodeId       int32 `json:"nodeid"`
	ServerInfo   string
	ChannelTypes []uint32
	ClusterId    int32
	AreaId       string
	NodeName     string
	PendingNodes []int32 // Pending for close subscription nodes for next KeepAlive
}

type SynerexGatewayInfo struct {
	NodeId       int32 `json:"nodeid"`
	GatewayInfo  string
	GatewayType  int32
	ChannelTypes []uint32
}

type nodeInfo struct {
	NodeId int32        `json:"nodeid"`
	Info   eachNodeInfo `json:"info"`
}

type srvNodeInfo struct {
	nodeMap map[int32]*eachNodeInfo // map from nodeID to eachNodeInfo
}

var (
	port       = flag.Int("port", getNodeservPort(), "Node Server Listening Port")
	addr       = flag.String("addr", getNodeservHostName(), "Node Server Listening Address")
	version    = flag.Bool("version", getVersion(), "show version")
	verbose    = flag.Bool("verbose", getVerbose(), "show detailed modules information")
	restart    = flag.Bool("restart", getRestart(), "Restart flag: if true, load nodeinfo.json ")
	srvInfo    srvNodeInfo
	sxProfile        = make([]SynerexServerInfo, 0, 1)
	lastNode   int32 = MaxServerID // start ID from MAX_SERVER_ID to MAX_NODE_NUM
	lastPrint  time.Time
	nmmu       sync.RWMutex
	srvprvfile string
)

// for embedding git variables
var (
	sha1ver   string // sha1 version used to build the program
	buildTime string // when the executable was built
	gitver    string // git release tag
)

func getNodeservPort() int {
	env := os.Getenv("SX_NODESERV_PORT")
	if env != "" {
		env, _ := strconv.Atoi(env)
		return env
	} else {
		return 9990
	}
}

func getNodeservHostName() string {
	env := os.Getenv("SX_NODESERV_HOST")
	if env != "" {
		return env
	} else {
		return "127.0.0.1"
	}
}

func getVersion() bool {
	env := os.Getenv("SX_NODESERV_VERSION")
	if env == "true" {
		return true
	} else {
		return false
	}
}

func getVerbose() bool {
	env := os.Getenv("SX_NODESERV_VERBOSE")
	if env == "true" {
		return true
	} else {
		return false
	}
}

func getRestart() bool {
	env := os.Getenv("SX_NODESERV_RESTART")
	if env == "true" {
		return true
	} else {
		return false
	}
}

func init() {
	//	log.Println("Starting Node Server..")
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
			// we need to check pending nodes (for Close Subscription)
			flag := true
			for i := range sxProfile {
				for j := range sxProfile[i].PendingNodes {
					if sxProfile[i].PendingNodes[j] == n {
						flag = false // still pending node!
						break
					}
				}
			}
			if flag {
				break
			}
		}
		if nodeType == nodepb.NodeType_SERVER {
			n = (n + 1) % MaxServerID
		} else {
			n = (n-MaxServerID+1)%(MaxNodeNum-MaxServerID) + MaxServerID
		}
		if n == lastNode || n == 0 { // loop
			nmmu.RUnlock()
			return -1 // all id is full...
		}
	}
	nmmu.RUnlock()
	if nodeType != nodepb.NodeType_SERVER {
		lastNode = n
	}
	return n
}

func loadSxProfile() {
	bytes, err := ioutil.ReadFile(defaultSxProfile)
	if err != nil {
		log.Println("Error on reading sxprofile.json ", err)
		return
	}
	jsonErr := json.Unmarshal(bytes, &sxProfile)
	if jsonErr != nil {
		log.Println("Can't unmarshall json ", jsonErr)
		return
	}
}
func loadNodeMap(s *srvNodeInfo) {
	nmmu.Lock() // not need..
	bytes, err := ioutil.ReadFile(defaultNodeInfoFile)
	if err != nil {
		log.Println("Error on reading nodeinfo.json ", err)
		return
	}
	nodeLists := make([]nodeInfo, 0)
	jsonErr := json.Unmarshal(bytes, &nodeLists)
	if jsonErr != nil {
		log.Println("Can't unmarshall json ", jsonErr)
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

func saveSxProfile() {
	bytes, err := json.MarshalIndent(sxProfile, "", "  ")
	if err != nil {
		log.Printf("Cant marshal sxprofile")
	}
	err = ioutil.WriteFile(defaultSxProfile, bytes, 0666)
	if err != nil {
		log.Println("Error on writing sxprofile.json ", err)
	}
}

// saving nodemap
func saveNodeMap(s *srvNodeInfo) {
	//	file, err  := os.OpenFile(defaultNodeInfoFile, os.O_CREATE,  )
	nmmu.Lock()
	nodeLists := make([]nodeInfo, len(s.nodeMap))
	i := 0
	for k, nif := range s.nodeMap {
		nodeLists[i] = nodeInfo{NodeId: k, Info: *nif}
		i++
	}
	bytes, err := json.MarshalIndent(nodeLists, "", "  ")
	if err != nil {
		log.Println("Can't Marshal NodeInfo data! ", err)
	} else {
		ferr := ioutil.WriteFile(defaultNodeInfoFile, bytes, 0666)
		if ferr != nil {
			log.Println("Error on writing nodeinfo.json ", ferr)
		}
		saveSxProfile()
	}
	nmmu.Unlock()
}

func appendNonDup(base []int32, add []int32) []int32 {
	for i := range add {
		flag := true
		for j := range base {
			if add[i] == base[j] {
				flag = false
				break
			}
		}
		if flag {
			base = append(base, add[i])
		}
	}
	return base
}

// add Pending Nodes of synerex-servers with killed nodes
// for non-keep alive providers
func addPendingNodesToServers(killNodes []int32) {
	for i := range sxProfile {
		sxProfile[i].PendingNodes = appendNonDup(sxProfile[i].PendingNodes, killNodes)
		fmt.Printf("SxProfile[%d] = %v", i, sxProfile[i].PendingNodes)
	}

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

		if len(killNodes) > 0 {
			// remove nodes
			// flush nodelist
			log.Printf("Kill Nodes by SynerexServer Timeout %#v", killNodes)
			for _, k := range killNodes {
				// we need to remove k from sxProfile
				ni := s.nodeMap[k]
				if ni.NodeType == nodepb.NodeType_SERVER { // remove server from sxProfile
					for jj, sv := range sxProfile {
						if sv.NodeId == k {
							sxProfile = append(sxProfile[:jj], sxProfile[jj+1:]...)
							break
						}
					}
				}
				delete(s.nodeMap, k)
			}
			// we need to notify killed nodes to synerex server to clean channels
			addPendingNodesToServers(killNodes)
		}
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
		log.Printf("%2d[%1d]%20s %-6.6s %-7.7s %14s %3d %2d:%3d %s\n", nk[i], eni.NodeType, eni.NodeName, eni.NodePBase, eni.NodeBinVersion, eni.Address, int(sub), eni.Count, eni.Status, eni.Arg)
	}
	nmmu.RUnlock()
}

// looking for Synerex Server for GW
func getSynerexServerForGw(ServerNames string) string {
	servers := strings.Split(ServerNames, ",")

	serverInfos := ""

	for i := range sxProfile {
		for j := range servers {
			if servers[j] == sxProfile[i].NodeName {
				if serverInfos != "" {
					serverInfos += ","
				}
				serverInfos += sxProfile[i].ServerInfo
			}
		}
	}
	return serverInfos
}

// looking for Synerex Server with given Id
func getSynerexServer(ServerId int32) string {
	for i := range sxProfile {
		if ServerId == sxProfile[i].NodeId {
			log.Printf("Server %d ServerInfo %s\n", ServerId, sxProfile[i].ServerInfo)
			return (sxProfile[i].ServerInfo)
		}
	}
	return ""
}

func (s *srvNodeInfo) RegisterNode(cx context.Context, ni *nodepb.NodeInfo) (nid *nodepb.NodeID, e error) {
	// registration
	n := int32(-1)
	if ni.WithNodeId == -1 {
		n = getNextNodeID(ni.NodeType)
	} else {
		// we need to check duplicate node_id
		_, ok := s.nodeMap[ni.WithNodeId]
		if ok {
			nn := getNextNodeID(ni.NodeType)
			log.Printf("Duplicated node ID request. Ignore %d and assign id %d", ni.WithNodeId, nn)
			n = nn
		} else {
			n = ni.WithNodeId
		}
	}

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
		NodeName:       ni.NodeName,
		NodePBase:      ni.NodePbaseVersion,
		NodeBinVersion: ni.BinVersion,
		NodeType:       ni.NodeType,
		Secret:         r,
		Address:        ipaddr,
		ServerInfo:     ni.ServerInfo,
		ChannelTypes:   ni.ChannelTypes,
		LastAlive:      time.Now(),

		Duration: DefaultDuration,
	}

	log.Println("Node Connection from :", ipaddr, ",", ni.NodeName)
	nmmu.Lock()

	s.nodeMap[n] = &eni
	if ni.NodeType == nodepb.NodeType_SERVER { // should register synerex_server profile.
		// check there is already that id
		existFlag := false
		for k, sx := range sxProfile {
			if sx.NodeId == n { // if there is same
				sxProfile[k].ServerInfo = ni.ServerInfo
				sxProfile[k].ChannelTypes = ni.ChannelTypes
				sxProfile[k].ClusterId = ni.ClusterId
				sxProfile[k].AreaId = ni.AreaId
				sxProfile[k].NodeName = ni.NodeName
				break
			}
		}
		if !existFlag { // no exist server
			sxProfile = append(sxProfile, SynerexServerInfo{
				NodeId:       n,
				ServerInfo:   ni.ServerInfo,
				ChannelTypes: ni.ChannelTypes,
				ClusterId:    ni.ClusterId,
				AreaId:       ni.AreaId,
				NodeName:     ni.NodeName,
				PendingNodes: []int32{},
			})
		}
	} else if ni.NodeType == nodepb.NodeType_GATEWAY { // gateway!

	}
	nmmu.Unlock()
	log.Println("------------------------------------------------------")
	s.listNodes()
	//	log.Println("------------------------------------------------------")

	// Getting Synerex Server to be connected to
	var ServerId int32 = 0
	if ni.NodeType == nodepb.NodeType_SERVER {
		ServerId = n
	} else if ni.NodeType == nodepb.NodeType_GATEWAY {
	} else {
		ServerId = GetServerIdForPrv(n)
	}

	serverInfo := ""

	if ni.NodeType == nodepb.NodeType_GATEWAY {
		serverInfo = getSynerexServerForGw(ni.GwInfo)
	} else {
		serverInfo = getSynerexServer(ServerId)
	}

	nid = &nodepb.NodeID{
		NodeId:            n,
		Secret:            r,
		ServerInfo:        serverInfo,
		KeepaliveDuration: eni.Duration,
	}
	saveNodeMap(s)

	if ni.NodeType == nodepb.NodeType_PROVIDER {
		UpdateConnectionMap(n, ServerId)
	}

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

	if ni.NodeType == nodepb.NodeType_SERVER { // if there is pending nodes, send them!
		//		log.Printf("KeepAlive from Server %#v", ni)
		for i := range sxProfile {
			if sxProfile[i].NodeId == nid {
				if len(sxProfile[i].PendingNodes) > 0 {
					bytes, _ := json.Marshal(sxProfile[i].PendingNodes)
					sxProfile[i].PendingNodes = []int32{} // clean nodes
					log.Printf("Sending Pending Nodes to SxServ %s", string(bytes))
					return &nodepb.Response{
						Ok:      true,
						Command: nodepb.KeepAliveCommand_PROVIDER_DISCONNECT,
						Err:     string(bytes),
					}, nil
				}
				break
			}
		}
	}
	// Returning SERVER_CHANGE command if threre is server change request for the provider
	if IsServerChangeRequest(nid) {
		log.Printf("Returning SERVER_CHANGE command\n")
		return &nodepb.Response{Ok: false, Command: nodepb.KeepAliveCommand_SERVER_CHANGE, Err: ""}, nil
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
	if ni.NodeType == nodepb.NodeType_SERVER { // this might be server
		for k, sx := range sxProfile {
			if sx.NodeId == n {
				sxProfile = append(sxProfile[:k], sxProfile[k+1:]...)
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

func (s *srvNodeInfo) QueryNodeInfos(cx context.Context, filter *nodecapi.NodeControlFilter) (ni *nodecapi.NodeControlInfos, e error) {
	var ninfo = make([]nodecapi.NodeControlInfo, 0, 1)

	var ServerId int32
	var ClusterId int32
	var AreaId string
	var NodeType nodepb.NodeType

	ns := nodecapi.NodeControlInfos{
		Infos: nil,
	}

	ns.Infos = make([]*nodecapi.NodeControlInfo, 0)

	all_flag := true
	if filter.NodeType == nodepb.NodeType_SERVER ||
		filter.NodeType == nodepb.NodeType_PROVIDER ||
		filter.NodeType == nodepb.NodeType_GATEWAY {
		all_flag = false
	}

	count := 0
	for n, nif := range s.nodeMap {
		if all_flag ||
			(filter.NodeType == nodepb.NodeType_PROVIDER &&
				nif.NodeType == nodepb.NodeType_PROVIDER) ||
			(filter.NodeType == nodepb.NodeType_SERVER &&
				nif.NodeType == nodepb.NodeType_SERVER) ||
			(filter.NodeType == nodepb.NodeType_GATEWAY &&
				nif.NodeType == nodepb.NodeType_GATEWAY) {

			ServerId = n
			ClusterId = 0
			AreaId = ""

			if nif.NodeType == nodepb.NodeType_PROVIDER {
				NodeType = nodepb.NodeType_PROVIDER
				ServerId = GetConnectSvrId(n)
			} else if nif.NodeType == nodepb.NodeType_SERVER {
				NodeType = nodepb.NodeType_SERVER
				for k, sx := range sxProfile {
					if sx.NodeId == n {
						ClusterId = sxProfile[k].ClusterId
						AreaId = sxProfile[k].AreaId
						break
					}
				}
			} else if nif.NodeType == nodepb.NodeType_SERVER {
				NodeType = nodepb.NodeType_GATEWAY
			}

			lastTime, _ := ptypes.TimestampProto(nif.LastAlive)
			ninfo = append(ninfo, nodecapi.NodeControlInfo{
				NodeInfo: &nodepb.NodeInfo{
					NodeName:         nif.NodeName,
					NodeType:         NodeType,
					ServerInfo:       nif.ServerInfo,
					NodePbaseVersion: nif.NodePBase,
					WithNodeId:       0,
					ClusterId:        ClusterId,
					AreaId:           AreaId,
					ChannelTypes:     nif.ChannelTypes,
					GwInfo:           "",
					BinVersion:       nif.NodeBinVersion,
					Count:            nif.Count,
					LastAliveTime:    lastTime,
					KeepaliveArg:     nif.Arg,
				},
				NodeId:   n,
				ServerId: ServerId,
			})
			ns.Infos = append(ns.Infos, &ninfo[count])
			count = count + 1
		}
	}

	return &ns, nil
}

func (s *srvNodeInfo) ControlNodes(ctx context.Context, in *nodecapi.Order) (res *nodecapi.NodeControlResponse, e error) {

	if in.OrderType == nodecapi.OrderType_SWITCH_SERVER {
		Provider := in.TargetNode.NodeId
		Server := in.GetSwitchInfo().SxServer.NodeId
		log.Printf("%d switch to %d\n", Provider, Server)
		AddServerChangeRequest(Provider, Server)
	}

	r := nodecapi.NodeControlResponse{
		Ok: true,
	}
	return &r, nil
}

func prepareGrpcServer(opts ...grpc.ServerOption) *grpc.Server {
	nodeServer := grpc.NewServer(opts...)
	nodepb.RegisterNodeServer(nodeServer, &srvInfo)
	nodecapi.RegisterNodeControlServer(nodeServer, &srvInfo)
	return nodeServer
}

func main() {
	// get debug information
	bi, ok := debug.ReadBuildInfo()
	flag.Parse()
	if ok {
		if *verbose {
			log.Printf("%s(%s) built %s sha1 %s", bi.Main.Path, gitver, buildTime, sha1ver)
			for i := range bi.Deps {
				m := bi.Deps[i]
				log.Printf("%d: %s(%s)", i, m.Path, m.Version)
			}

		} else {
			log.Printf("%s(%s) built %s sha1 %s", bi.Main.Path, gitver, buildTime, sha1ver)
		}
		if *version {
			os.Exit(0)
		}
		//
	} else {
		log.Printf("Can't obtain build info.")
	}

	if gerr := agent.Listen(agent.Options{}); gerr != nil {
		log.Fatal(gerr)
	}

	// loading nodeinfo from file
	if *restart {
		log.Printf("loading nodeinfo.json..\n")
		loadNodeMap(&srvInfo)
		srvInfo.listNodes()
	}

	//	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *addr, *port))

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption

	nodeServer := prepareGrpcServer(opts...)

	log.Printf("Starting Node Server: Waiting Connection at port :%d ...", *port)

	nodeServer.Serve(lis)
}
