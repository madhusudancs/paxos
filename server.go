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
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
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

type ReplicaConfig struct {
	Cluster NetAddr
	Client  NetAddr
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
	IsRunning    bool
	AmITheLeader bool
	Leader       string
}

var (
	thisId       string
	prepare_id   int32
	data         Data
	lastAccepted Accepted
)

func main() {
	flags := flagDef()
	config := getConfig("config.yml")
	thisId = flags.id

	if _, ok := config.Replicas[thisId]; !ok {
		panic(fmt.Sprintf("No configuration defined for ID: \"%s\"\n", thisId))
	}

	thisClusterNetAddr := config.Replicas[thisId].Cluster
	thisClientNetAddr := config.Replicas[thisId].Client

	prepare_id = 0
	go startCoordinator(thisClusterNetAddr)
	go startServer(thisClientNetAddr)
	go paxos(flags, config)

	tick := time.Tick(config.Heartbeat.Interval * time.Millisecond)
	heartBeatSkipCount := 0
	for _ = range tick {
		log.Printf("Leader now is: %s\n", data.Leader)
		if testLiveness(config) {
			heartBeatSkipCount = 0
			continue
		}
		heartBeatSkipCount++
		if heartBeatSkipCount >= config.Heartbeat.Max {
			data.Leader = ""
			data.AmITheLeader = false
			go paxos(flags, config)
		}
	}
}

func testLiveness(config Config) bool {
	if data.AmITheLeader {
		return true
	}
	heartBeatMessage := &message.Message{
		Type: message.Message_HEARTBEAT.Enum(),
	}
	serializedHeartBeatMessage, err := proto.Marshal(heartBeatMessage)
	if err != nil {
		log.Println("Marshaling error: ", err)
		return false
	}

	conn, err := udpDial(config.Replicas[data.Leader].Cluster)

	if err != nil {
		log.Printf("Dialing to %s failed %+v\n", data.Leader, err)
		// handle error
		return false
	}

	timeOut := (config.Heartbeat.Interval * time.Millisecond) / 3
	conn.SetWriteDeadline(time.Now().Add(timeOut))
	_, err = conn.Write(serializedHeartBeatMessage)
	if err != nil {
		log.Println("Heartbeat send failed")
		return false
	}

	readBuf := make([]byte, BufSize)
	conn.SetReadDeadline(time.Now().Add(timeOut))
	_, err = conn.Read(readBuf)
	if err != nil {
		log.Println("Heartbeat ACK receive failed")
		return false
	}
	heartBeatAckMessage := &message.Message{}
	proto.Unmarshal(readBuf, heartBeatAckMessage)

	if *heartBeatAckMessage.Type == message.Message_HEARTBEAT_ACK {
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

	flags := Flags{*id, *dataDir}

	return flags
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
	var config Config
	goyaml.Unmarshal(configBytes, &config)
	return config
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

func startCoordinator(netAddr NetAddr) {
	conn, err := udpListen(netAddr)
	if err != nil {
		panic(err)
	}
	log.Printf("Coordinator server started on %+v\n", netAddr)

	go handleCoordinationMessage(*conn)
}

func startServer(netAddr NetAddr) {
	ln, err := tcpListen(netAddr)
	if err != nil {
		panic(err)
	}
	log.Printf("Client server started on %+v\n", netAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			continue
		}
		go handleClientMessage(conn)
	}
}

func paxos(flags Flags, config Config) {
	// If an instance of Paxos is running we should not run it again
	if data.IsRunning {
		return
	}

	data.IsRunning = true

	numReplicas := len(config.Replicas)
	quorum := (numReplicas / 2) + 1

	log.Println("Bootstrapping ...")

	for data.Leader == "" {
		prepare_id++
		prepareMessage := &message.Message{
			Type: message.Message_PREPARE.Enum(),
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
			conn, err := udpDial(replica.Cluster)

			if err != nil {
				log.Printf("Dialing to %s failed %+v\n", id, err)
				// handle error
				continue
			}
			go prepare(id, promiseChan, conn, serializedPrepareMessage, config)
		}

		log.Println("Prepare messages sent")

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
		log.Println("Promise quorum received")

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

		requestMessage := &message.Message{
			Type: message.Message_REQUEST.Enum(),
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
			conn, err := udpDial(replica.Cluster)

			if err != nil {
				log.Printf("Dialing to %s failed %+v\n", id, err)
				// handle error
				continue
			}
			go request(id, acceptChan, conn, serializedRequestMessage, config)
		}

		log.Println("Request sent")

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
		log.Println("Accept messages received")

		if restart {
			data.Leader = ""
			data.AmITheLeader = false
			time.Sleep(config.Heartbeat.Interval * time.Millisecond)
		} else {
			break
		}
	}
	data.IsRunning = false
	log.Printf("Finally leader elected! And is: %+v\n", data.Leader)
}

func prepare(id string, promiseChan chan message.Promise, conn net.Conn, serializedPrepareMessage []byte, config Config) {
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
	promiseMessage := &message.Message{}
	proto.Unmarshal(readBuf, promiseMessage)
	log.Printf("Promise message received: %s %+v\n", id, promiseMessage)
	promiseChan <- *promiseMessage.Promise
}

func request(id string, acceptChan chan message.Accept, conn net.Conn, serializedRequestMessage []byte, config Config) {
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
	acceptMessage := &message.Message{}
	proto.Unmarshal(readBuf, acceptMessage)
	log.Printf("Accept message received: %s %+v\n", id, acceptMessage)
	acceptChan <- *acceptMessage.Accept
}

func handleCoordinationMessage(conn net.UDPConn) {
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

		coordinationMessage := &message.Message{}
		proto.Unmarshal(readBuf, coordinationMessage)

		switch *coordinationMessage.Type {
		case message.Message_PREPARE:
			go promise(conn, clientAddr, *coordinationMessage.Prepare)
		case message.Message_REQUEST:
			go accept(conn, clientAddr, *coordinationMessage.Request)
		case message.Message_HEARTBEAT:
			go heartbeatAck(conn, clientAddr)
		}
	}
}

func promise(conn net.UDPConn, clientAddr *net.UDPAddr, prepareMessage message.Prepare) {
	log.Printf("Prepare message received: %+v\n", prepareMessage)
	var promiseMessage *message.Message
	if data.AmITheLeader {
		promiseMessage = &message.Message{
			Type: message.Message_PROMISE.Enum(),
			Promise: &message.Promise{
				Ack:    proto.Bool(false),
				Leader: proto.String(data.Leader),
			},
		}
	} else if *prepareMessage.Id > lastAccepted.Id {
		promiseMessage = &message.Message{
			Type: message.Message_PROMISE.Enum(),
			Promise: &message.Promise{
				Ack:               proto.Bool(true),
				LastAcceptedValue: proto.Int32(lastAccepted.Value),
				LastAccepted: &message.Prepare{
					ProposerId: proto.String(lastAccepted.ProposerId),
					Id:         proto.Int32(lastAccepted.Id),
				},
			},
		}
		lastAccepted.ProposerId = *prepareMessage.ProposerId
		lastAccepted.Id = *prepareMessage.Id
	} else {
		promiseMessage = &message.Message{
			Type: message.Message_PROMISE.Enum(),
			Promise: &message.Promise{
				Ack:               proto.Bool(false),
				LastAcceptedValue: proto.Int32(lastAccepted.Value),
				LastAccepted: &message.Prepare{
					ProposerId: proto.String(lastAccepted.ProposerId),
					Id:         proto.Int32(lastAccepted.Id),
				},
			},
		}
	}
	serializedPromiseMessage, err := proto.Marshal(promiseMessage)
	if err != nil {
		log.Println("Marshaling error: ", err)
		return
	}
	conn.WriteToUDP(serializedPromiseMessage, clientAddr)
}

func accept(conn net.UDPConn, clientAddr *net.UDPAddr, requestMessage message.Request) {
	log.Printf("Request message received: %+v\n", requestMessage)
	var acceptMessage *message.Message
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
		log.Printf("Accepting leadership of %+v\n", data.Leader)
	} else {
		ack = false
	}
	acceptMessage = &message.Message{
		Type: message.Message_ACCEPT.Enum(),
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
	heartbeatAckMessage := &message.Message{
		Type: message.Message_HEARTBEAT_ACK.Enum(),
	}

	serializedHeartbeatAckMessage, err := proto.Marshal(heartbeatAckMessage)
	if err != nil {
		log.Println("Marshaling error: ", err)
		return
	}
	conn.WriteToUDP(serializedHeartbeatAckMessage, clientAddr)
}

func handleClientMessage(conn net.Conn) {
	readBuf := make([]byte, BufSize)
	conn.Read(readBuf)
	fmt.Printf("read: %+v\n", string(readBuf))
}
