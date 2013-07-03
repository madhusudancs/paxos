/*
 * Copyright 2013 Madhusudan C.S.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"sync"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"launchpad.net/goyaml"

	"./message"
)

const BufSize = 4096

type Flags struct {
	id      string
	dataDir string
}

type HeartbeatConfig struct {
	Interval time.Duration
	Max      int
}

type NetAddr struct {
	Host string
	Port int
}

type Listeners struct {
	Message NetAddr
	Data    NetAddr
}

type ReplicaConfig struct {
	Coordinator Listeners
	Server      Listeners
}

type Config struct {
	Heartbeat HeartbeatConfig
	Replicas  map[string]ReplicaConfig
}

type Accepted struct {
	ProposerId string
	Id         int32
	Value      int32
}

type Data struct {
	RunningCond  *sync.Cond
	IsRunning    bool
	AmITheLeader bool
	Leader       string
}

var (
	flags        Flags
	config       Config
	thisId       string
	prepare_id   int32
	data         Data
	lastAccepted Accepted
	connections  map[string]net.Conn
	storedData   [][]byte
)

func main() {
	flags = flagDef()
	config = getConfig("config.yml")
	thisId = flags.id
	storedData = make([][]byte, 0)

	var l sync.Mutex
	data.RunningCond = sync.NewCond(&l)

	if _, ok := config.Replicas[thisId]; !ok {
		panic(fmt.Sprintf("No configuration defined for ID: \"%s\"\n", thisId))
	}

	prepare_id = 0
	go startMessageCoordinator(config.Replicas[thisId].Coordinator.Message)
	go startDataCoordinator(config.Replicas[thisId].Coordinator.Data)
	go startMessageServer(config.Replicas[thisId].Server.Message)
	go startDataServer(config.Replicas[thisId].Server.Data)
	go paxos(flags)

	tick := time.Tick(config.Heartbeat.Interval * time.Millisecond)
	heartBeatSkipCount := 0
	for _ = range tick {
		if testLiveness() {
			heartBeatSkipCount = 0
			continue
		}
		heartBeatSkipCount++
		if heartBeatSkipCount >= config.Heartbeat.Max {
			data.Leader = ""
			data.AmITheLeader = false
			go paxos(flags)
		}
	}
}

func testLiveness() bool {
	if data.AmITheLeader {
		return true
	}
	heartBeatMessage := &message.CoordinationMessage{
		Type: message.CoordinationMessage_HEARTBEAT.Enum(),
	}
	serializedHeartBeatMessage, err := proto.Marshal(heartBeatMessage)
	if err != nil {
		log.Println("Marshaling error: ", err)
		return false
	}

	conn, err := udpDial(config.Replicas[data.Leader].Coordinator.Message)

	if err != nil {
		log.Printf("Dialing to %s failed %+v\n", data.Leader, err)
		// handle error
		return false
	}

	timeOut := (config.Heartbeat.Interval * time.Millisecond) / 3
	conn.SetWriteDeadline(time.Now().Add(timeOut))
	_, err = conn.Write(serializedHeartBeatMessage)
	if err != nil {
		return false
	}

	readBuf := make([]byte, BufSize)
	conn.SetReadDeadline(time.Now().Add(timeOut))
	_, err = conn.Read(readBuf)
	if err != nil {
		return false
	}
	heartBeatAckMessage := &message.CoordinationMessage{}
	proto.Unmarshal(readBuf, heartBeatAckMessage)

	if *heartBeatAckMessage.Type == message.CoordinationMessage_HEARTBEAT_ACK {
		return true
	}
	return false
}

func flagDef() Flags {
	// Define the flags that this program can accept.
	var id = flag.String("id", "", "ID of this replica")
	var dataDir = flag.String("data", "", "Path to the data directory")

	// Parse the flags
	flag.Parse()

	return Flags{*id, *dataDir}
}

func readConfigFile(fileName string) []byte {
	// Read the input statements from the given file.
	body, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Print("File reading error")
	}

	// Return the content of the read file.
	return body
}

func getConfig(fileName string) Config {
	configBytes := readConfigFile(fileName)
	var configLocal Config
	goyaml.Unmarshal(configBytes, &configLocal)
	return configLocal
}

func udpListen(netAddr NetAddr) (*net.UDPConn, error) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", netAddr.Host, netAddr.Port))
	if err != nil {
		return nil, err
	}
	return net.ListenUDP("udp", addr)
}

func udpDial(netAddr NetAddr) (net.Conn, error) {
	return net.Dial("udp", fmt.Sprintf("%s:%d", netAddr.Host, netAddr.Port))
}

func tcpListen(netAddr NetAddr) (net.Listener, error) {
	return net.Listen("tcp", fmt.Sprintf("%s:%d", netAddr.Host, netAddr.Port))
}

func tcpDial(netAddr NetAddr) (net.Conn, error) {
	return net.Dial("tcp", fmt.Sprintf("%s:%d", netAddr.Host, netAddr.Port))
}

func startMessageCoordinator(netAddr NetAddr) {
	conn, err := udpListen(netAddr)
	if err != nil {
		panic(err)
	}
	log.Printf("Coordinator message server started on %+v\n", netAddr)

	go handleCoordinatorMessage(*conn)
}

func startDataCoordinator(netAddr NetAddr) {
	ln, err := tcpListen(netAddr)
	if err != nil {
		panic(err)
	}
	log.Printf("Coordinator data server started on %+v\n", netAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			continue
		}
		go handleCoordinatorData(conn)
	}
}

func startMessageServer(netAddr NetAddr) {
	conn, err := udpListen(netAddr)
	if err != nil {
		panic(err)
	}
	log.Printf("Client message server started on %+v\n", netAddr)

	go handleClientMessage(*conn)
}

func startDataServer(netAddr NetAddr) {
	ln, err := tcpListen(netAddr)
	if err != nil {
		panic(err)
	}
	log.Printf("Client data server started on %+v\n", netAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			continue
		}
		go handleClientData(conn)
	}
}

func paxos(flags Flags) {
	// If an instance of Paxos is running we should not run it again
	if data.IsRunning {
		return
	}

	data.IsRunning = true

	numReplicas := len(config.Replicas)
	quorum := (numReplicas / 2) + 1

	for data.Leader == "" {
		prepare_id++
		prepareMessage := &message.CoordinationMessage{
			Type: message.CoordinationMessage_PREPARE.Enum(),
			Prepare: &message.Prepare{
				ProposerId: proto.String(thisId),
				Id:         proto.Int32(prepare_id),
			},
		}
		serializedPrepareMessage, err := proto.Marshal(prepareMessage)
		if err != nil {
			log.Println("Marshaling error: ", err)
			continue
		}

		promiseChan := make(chan message.Promise, quorum)
		for id, replica := range config.Replicas {
			conn, err := udpDial(replica.Coordinator.Message)

			if err != nil {
				log.Printf("Dialing to %s failed %+v\n", id, err)
				// handle error
				continue
			}
			go prepare(id, promiseChan, conn, serializedPrepareMessage)
		}

		allPromiseMessages := make([]message.Promise, 0)
		promiseMessages := make([]message.Promise, 0)

		terminate := true
		for i := 0; i < numReplicas; i++ {
			promiseMessage := <-promiseChan
			allPromiseMessages = append(promiseMessages, promiseMessage)
			if *promiseMessage.Ack {
				promiseMessages = append(promiseMessages, promiseMessage)
				if len(promiseMessages) >= quorum {
					data.Leader = thisId
					data.AmITheLeader = true
					terminate = false
					break
				}
			} else if promiseMessage.Leader != nil && *promiseMessage.Leader != "" {
				data.Leader = *promiseMessage.Leader
				data.AmITheLeader = false
				// There is a leader candidate, so wait for a tick to see if the candidate is really chosen
				terminate = true
				break
			}
		}

		if terminate {
			break
		}

		for loop := true; loop && len(allPromiseMessages) < numReplicas; {
			tick := time.Tick(config.Heartbeat.Interval * time.Millisecond)
			select {
			case promiseMessage := <-promiseChan:
				allPromiseMessages = append(promiseMessages, promiseMessage)
				if *promiseMessage.Ack {
					promiseMessages = append(promiseMessages, promiseMessage)
				}
			case <-tick:
				loop = false
			}
		}

		maxId := int32(-1)
		maxValue := int32(0)
		for _, promiseMessage := range promiseMessages {
			if *promiseMessage.LastAccepted.Id > maxId {
				maxId = *promiseMessage.LastAccepted.Id
				maxValue = *promiseMessage.LastAcceptedValue
			}
		}

		requestMessage := &message.CoordinationMessage{
			Type: message.CoordinationMessage_REQUEST.Enum(),
			Request: &message.Request{
				Prepare: &message.Prepare{
					ProposerId: proto.String(thisId),
					Id:         proto.Int32(prepare_id),
				},
				Value: proto.Int32(maxValue),
			},
		}
		serializedRequestMessage, err := proto.Marshal(requestMessage)
		if err != nil {
			log.Println("Marshaling error: ", err)
		}

		acceptChan := make(chan message.Accept, quorum)

		for id, replica := range config.Replicas {
			conn, err := udpDial(replica.Coordinator.Message)

			if err != nil {
				log.Printf("Dialing to %s failed %+v\n", id, err)
				// handle error
				continue
			}
			go request(id, acceptChan, conn, serializedRequestMessage)
		}

		restart := true
		acceptCount := 0
		for _ = range config.Replicas {
			acceptMessage := <-acceptChan
			if *acceptMessage.Ack {
				acceptCount++
				if acceptCount >= quorum {
					restart = false
					break
				}
			}
		}

		if restart {
			data.Leader = ""
			data.AmITheLeader = false
			time.Sleep(config.Heartbeat.Interval * time.Millisecond)
		} else {
			break
		}
	}
	data.IsRunning = false
	data.RunningCond.Broadcast()
}

func prepare(id string, promiseChan chan message.Promise, conn net.Conn, serializedPrepareMessage []byte) {
	timeOut := (config.Heartbeat.Interval * time.Millisecond) * time.Duration(config.Heartbeat.Max)

	conn.SetWriteDeadline(time.Now().Add(timeOut))
	_, err := conn.Write(serializedPrepareMessage)
	if err != nil {
		log.Println("Prepare send failed")
		promiseChan <- message.Promise{
			Ack: proto.Bool(false),
		}
		return
	}

	readBuf := make([]byte, BufSize)
	conn.SetReadDeadline(time.Now().Add(timeOut))
	_, err = conn.Read(readBuf)
	if err != nil {
		log.Println("Promise receive failed")
		promiseChan <- message.Promise{
			Ack: proto.Bool(false),
		}
		return
	}
	promiseMessage := &message.CoordinationMessage{}
	proto.Unmarshal(readBuf, promiseMessage)
	promiseChan <- *promiseMessage.Promise
}

func request(id string, acceptChan chan message.Accept, conn net.Conn, serializedRequestMessage []byte) {
	timeOut := (config.Heartbeat.Interval * time.Millisecond) * time.Duration(config.Heartbeat.Max)

	conn.SetWriteDeadline(time.Now().Add(timeOut))
	_, err := conn.Write(serializedRequestMessage)
	if err != nil {
		log.Println("Request send failed")
		acceptChan <- message.Accept{
			Ack: proto.Bool(false),
		}
		return
	}

	readBuf := make([]byte, BufSize)
	conn.SetReadDeadline(time.Now().Add(timeOut))
	_, err = conn.Read(readBuf)
	if err != nil {
		log.Println("Accept receive failed", err)
		acceptChan <- message.Accept{
			Ack: proto.Bool(false),
		}
		return
	}
	acceptMessage := &message.CoordinationMessage{}
	proto.Unmarshal(readBuf, acceptMessage)
	acceptChan <- *acceptMessage.Accept
}

func handleCoordinatorMessage(conn net.UDPConn) {
	for {
		readBuf := make([]byte, BufSize)
		_, clientAddr, err := conn.ReadFromUDP(readBuf)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("Coordination message reading error", err)
			continue
		}

		coordinationMessage := &message.CoordinationMessage{}
		proto.Unmarshal(readBuf, coordinationMessage)

		switch *coordinationMessage.Type {
		case message.CoordinationMessage_PREPARE:
			go promise(conn, clientAddr, *coordinationMessage.Prepare)
		case message.CoordinationMessage_REQUEST:
			go accept(conn, clientAddr, *coordinationMessage.Request)
		case message.CoordinationMessage_HEARTBEAT:
			go heartbeatAck(conn, clientAddr)
		}
	}
}

func promise(conn net.UDPConn, clientAddr *net.UDPAddr, prepareMessage message.Prepare) {
	var ack bool
	promiseMessage := &message.CoordinationMessage{
		Type:    message.CoordinationMessage_PROMISE.Enum(),
		Promise: &message.Promise{},
	}

	if data.AmITheLeader {
		ack = false
		promiseMessage.Promise.Leader = proto.String(data.Leader)
	} else if *prepareMessage.Id > lastAccepted.Id {
		ack = true
		promiseMessage.Promise.LastAcceptedValue = proto.Int32(lastAccepted.Value)
		promiseMessage.Promise.LastAccepted = &message.Prepare{
			ProposerId: proto.String(lastAccepted.ProposerId),
			Id:         proto.Int32(lastAccepted.Id),
		}

		lastAccepted.ProposerId = *prepareMessage.ProposerId
		lastAccepted.Id = *prepareMessage.Id
	} else {
		ack = false
		promiseMessage.Promise.LastAcceptedValue = proto.Int32(lastAccepted.Value)
		promiseMessage.Promise.LastAccepted = &message.Prepare{
			ProposerId: proto.String(lastAccepted.ProposerId),
			Id:         proto.Int32(lastAccepted.Id),
		}
	}
	promiseMessage.Promise.Ack = proto.Bool(ack)
	serializedPromiseMessage, err := proto.Marshal(promiseMessage)
	if err != nil {
		log.Println("Marshaling error: ", err)
		return
	}
	conn.WriteToUDP(serializedPromiseMessage, clientAddr)
}

func accept(conn net.UDPConn, clientAddr *net.UDPAddr, requestMessage message.Request) {
	var acceptMessage *message.CoordinationMessage
	ack := false
	if data.AmITheLeader {
		if thisId == *requestMessage.Prepare.ProposerId {
			ack = true
		} else {
			ack = false
		}
	} else if *requestMessage.Prepare.Id == lastAccepted.Id && *requestMessage.Prepare.ProposerId == lastAccepted.ProposerId {
		ack = true
		data.Leader = lastAccepted.ProposerId
		data.AmITheLeader = false
		lastAccepted.Value = *requestMessage.Value
	} else {
		ack = false
	}
	acceptMessage = &message.CoordinationMessage{
		Type: message.CoordinationMessage_ACCEPT.Enum(),
		Accept: &message.Accept{
			Ack: proto.Bool(ack),
		},
	}
	serializedAcceptMessage, err := proto.Marshal(acceptMessage)
	if err != nil {
		log.Println("Marshaling error: ", err)
		return
	}
	conn.WriteToUDP(serializedAcceptMessage, clientAddr)
}

func heartbeatAck(conn net.UDPConn, clientAddr *net.UDPAddr) {
	if !data.AmITheLeader {
		return
	}
	heartbeatAckMessage := &message.CoordinationMessage{
		Type: message.CoordinationMessage_HEARTBEAT_ACK.Enum(),
	}

	serializedHeartbeatAckMessage, err := proto.Marshal(heartbeatAckMessage)
	if err != nil {
		log.Println("Marshaling error: ", err)
		return
	}
	conn.WriteToUDP(serializedHeartbeatAckMessage, clientAddr)
}

func handleClientMessage(conn net.UDPConn) {
	for {
		readBuf := make([]byte, BufSize)
		_, clientAddr, err := conn.ReadFromUDP(readBuf)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("Coordination message reading error", err)
			continue
		}

		getLeaderMessage := &message.GetLeader{}
		proto.Unmarshal(readBuf, getLeaderMessage)

		replyLeaderMessage := &message.ReplyLeader{}

		if data.Leader != "" {
			replyLeaderMessage.Leader = proto.String(data.Leader)
		}

		serializedReplyLeaderMessage, err := proto.Marshal(replyLeaderMessage)
		if err != nil {
			log.Println("Marshaling error: ", err)
			return
		}
		conn.WriteToUDP(serializedReplyLeaderMessage, clientAddr)
	}
}

func handleClientData(conn net.Conn) {
	for {
		readBuf := make([]byte, BufSize)
		_, err := conn.Read(readBuf)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("Client data reading error", err)
			continue
		}

		clientDataMessage := &message.ClientData{}
		proto.Unmarshal(readBuf, clientDataMessage)

		clientDataAckMessage := &message.ClientDataAck{}

		_, err = quorumWrite(clientDataMessage.Data)
		if err != nil {
			clientDataAckMessage.Ack = proto.Bool(false)
		} else {
			clientDataAckMessage.Ack = proto.Bool(true)
		}

		serializedClientDataAckMessage, err := proto.Marshal(clientDataAckMessage)
		if err != nil {
			log.Println("Marshaling error: ", err)
			return
		}
		conn.Write(serializedClientDataAckMessage)
	}
}

func quorumWrite(clientData []byte) (int, error) {
	numReplicas := len(config.Replicas)
	quorum := (numReplicas / 2) + 1

	data.RunningCond.L.Lock()
	for data.IsRunning {
		data.RunningCond.Wait()
	}
	data.RunningCond.L.Unlock()

	if !data.AmITheLeader {
		return 0, errors.New("I am not the leader.")
	}

	coordinatorDataMessage := &message.CoordinatorData{
		Leader: proto.String(thisId),
		Data:   clientData,
	}

	serializedCoordinatorDataMessage, err := proto.Marshal(coordinatorDataMessage)
	if err != nil {
		log.Println("Marshaling error: ", err)
		return 0, err
	}

	dataAckChan := make(chan message.CoordinatorDataAck, quorum)
	for id, replica := range config.Replicas {
		conn, ok := connections[id]
		if !ok {
			conn, err = tcpDial(replica.Coordinator.Data)
			if err != nil {
				log.Printf("TCP dialing to %s failed %+v\n", id, err)
				// handle error
				continue
			}
		}

		go replicaWrite(dataAckChan, serializedCoordinatorDataMessage, conn)
	}

	log.Println("Replica writes sent")

	ackCount := 0
	for _ = range config.Replicas {
		coordinatorDataAckMessage := <-dataAckChan
		log.Printf("Coordinator Data Ack Message: %+v\n", coordinatorDataAckMessage)
		if *coordinatorDataAckMessage.Ack {
			ackCount++
			if ackCount >= quorum {
				log.Println("Coordinator Data Ack Quorum reached")
				return len(clientData), nil
			}
		}
	}
	return 0, errors.New("Quorum write failed")
}

func replicaWrite(dataAckChan chan message.CoordinatorDataAck, serializedCoordinatorDataMessage []byte, conn net.Conn) {
	timeOut := (config.Heartbeat.Interval * time.Millisecond) * time.Duration(config.Heartbeat.Max)

	conn.SetWriteDeadline(time.Now().Add(timeOut))
	_, err := conn.Write(serializedCoordinatorDataMessage)
	if err != nil {
		log.Println("CoordinatorData send failed")
		dataAckChan <- message.CoordinatorDataAck{
			Ack: proto.Bool(false),
		}
		return
	}

	readBuf := make([]byte, BufSize)
	conn.SetReadDeadline(time.Now().Add(timeOut))
	_, err = conn.Read(readBuf)
	if err != nil {
		log.Println("CoordinatorDataAck receive failed", err)
		dataAckChan <- message.CoordinatorDataAck{
			Ack: proto.Bool(false),
		}
		return
	}
	coordinatorDataAckMessage := &message.CoordinatorDataAck{}
	proto.Unmarshal(readBuf, coordinatorDataAckMessage)
	dataAckChan <- *coordinatorDataAckMessage
}

func handleCoordinatorData(conn net.Conn) {
	for {
		readBuf := make([]byte, BufSize)
		_, err := conn.Read(readBuf)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("Client data reading error", err)
			continue
		}

		coordinatorDataMessage := &message.CoordinatorData{}
		proto.Unmarshal(readBuf, coordinatorDataMessage)

		coordinatorDataAckMessage := &message.CoordinatorDataAck{}

		_, err = localWrite(coordinatorDataMessage.Data)
		if err != nil {
			coordinatorDataAckMessage.Ack = proto.Bool(false)
		} else {
			coordinatorDataAckMessage.Ack = proto.Bool(true)
		}

		serializedCoordinatorDataAckMessage, err := proto.Marshal(coordinatorDataAckMessage)
		if err != nil {
			log.Println("Marshaling error: ", err)
			return
		}
		conn.Write(serializedCoordinatorDataAckMessage)
	}
}

func localWrite(coordinatorData []byte) (int, error) {
	storedData = append(storedData, coordinatorData)
	return len(coordinatorData), nil
}
