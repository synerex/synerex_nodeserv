package main

import (
	//	"context"
	"log"
	//	nodecapi "github.com/synerex/synerex_nodeserv_controlapi"
)

type NodeServInfo struct {
	PrvNodeId int32
	SrvNodeId int32
}

type ChangeServInfo struct {
	PrvId int32
	SrvId int32
}

var ConnectionMap = make([]NodeServInfo, 0, 1)
var ChangeSrvList = make([]ChangeServInfo, 0, 1)

func UpdateConnectionMap(PrvId int32, SrvId int32) {

	existFlag := false
	for ii := range ConnectionMap {
		if ConnectionMap[ii].PrvNodeId == PrvId {
			ConnectionMap[ii].SrvNodeId = SrvId
			existFlag = true
			break
		}
	}
	if !existFlag {
		ConnectionMap = append(ConnectionMap, NodeServInfo{
			PrvNodeId: PrvId,
			SrvNodeId: SrvId})
	}

}

func GetConnectSvrId(PrvId int32) int32 {

	for ii := range ConnectionMap {
		if PrvId == ConnectionMap[ii].PrvNodeId {
			return ConnectionMap[ii].SrvNodeId
		}
	}
	return 0
}

func GetServerIdForPrv(PrvId int32) int32 {

	for k := range ChangeSrvList {
		if PrvId == ChangeSrvList[k].PrvId {
			SrvId := ChangeSrvList[k].SrvId
			ChangeSrvList = append(ChangeSrvList[:k], ChangeSrvList[k+1:]...)
			return (SrvId)
		}
	}
	if len(sxProfile) > 0 {
		return sxProfile[0].NodeId
	}
	return 0 // default server is 0.
}

func IsServerChangeRequest(PrvId int32) bool {

	for k := range ChangeSrvList {
		if PrvId == ChangeSrvList[k].PrvId {
			log.Printf("ServerChangeRequest for %d connected to %d\n ",
				ChangeSrvList[k].PrvId, ChangeSrvList[k].SrvId)
			return true
		}
	}
	return false
}

func AddServerChangeRequest(PrvId, SrvId int32) {
	ChangeSrvList = append(ChangeSrvList, ChangeServInfo{
		PrvId: PrvId,
		SrvId: SrvId,
	})
}
