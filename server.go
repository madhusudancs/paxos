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
	AmITheLeader bool
	Leader       string
}

var (
	data         Data
	lastAccepted Accepted
)

func main() {
	flags := flagDef()
	config := getConfig("config.yml")
	thisId := flags.id

	if _, ok := config.Replicas[thisId]; !ok {
		panic(fmt.Sprintf("No configuration defined for ID: \"%s\"\n", thisId))
	}

	thisClusterNetAddr := config.Replicas[thisId].Cluster
	thisClientNetAddr := config.Replicas[thisId].Client

	go startCoordinator(thisClusterNetAddr)
	go startServer(thisClientNetAddr)
	go paxos(flags, config)
	tick := time.Tick(config.Heartbeat.Interval * time.Millisecond)
	for _ = range tick {
		log.Printf("Leader now is: %s\n", data.Leader)
	}
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

func tcpListen(netAddr NetAddr) (net.Listener, error) {
	return net.Listen("tcp", fmt.Sprintf("%s:%d", netAddr.Host, netAddr.Port))
}

func tcpDial(netAddr NetAddr) (net.Conn, error) {
	return net.Dial("tcp", fmt.Sprintf("%s:%d", netAddr.Host, netAddr.Port))
}

func startCoordinator(netAddr NetAddr) {
	ln, err := tcpListen(netAddr)
	if err != nil {
		panic(err)
	}
	log.Printf("Coordinator server started on %+v\n", netAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Connection Accepting error")
			// handle error
			continue
		}
		go handleCoordinationMessage(conn)
	}
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
	numReplicas := len(config.Replicas)
	quorum := (numReplicas / 2) + 1
	prepare_id := int32(0)
	thisId := flags.id
	connections := make(map[string]net.Conn)

	promiseChan := make(chan message.Promise, quorum)

	var promiseMessage message.Promise
	allPromiseMessages := make([]message.Promise, 0)
	promiseMessages := make([]message.Promise, 0)

	acceptChan := make(chan message.Accept, quorum)

	var acceptMessage message.Accept

	log.Println("Bootstrapping ...")

	tick := time.Tick(config.Heartbeat.Interval * time.Millisecond)
	for _ = range tick {
		if data.Leader != "" {
			break
		}
		for id, replica := range config.Replicas {
			if _, ok := connections[id]; ok {
				continue
			}

			conn, err := tcpDial(replica.Cluster)

			if err != nil {
				log.Printf("Dialing to %s failed %+v\n", id, err)
				// handle error
				continue
			}
			log.Printf("Connected to %s ...\n", id)
			connections[id] = conn
		}

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

		if len(connections) < quorum {
			continue
		}

		for _, conn := range connections {
			go prepare(promiseChan, conn, serializedPrepareMessage, config)
		}

		restart := true
		for i := 0; i < numReplicas; i++ {
			promiseMessage = <-promiseChan
			allPromiseMessages = append(promiseMessages, promiseMessage)
			if *promiseMessage.Ack {
				promiseMessages = append(promiseMessages, promiseMessage)
				if len(promiseMessages) >= quorum {
					data.Leader = thisId
					data.AmITheLeader = true
					restart = false
					break
				}
			} else if promiseMessage.Leader != nil && *promiseMessage.Leader != "" {
				restart = true
				// There is a leader candidate, so wait for a tick to see if the candidate is really chosen
				break
			}
		}

		if restart {
			continue
		}

		for tickTimeout := config.Heartbeat.Max; tickTimeout > 0 && len(allPromiseMessages) < numReplicas; {
			select {
			case promiseMessage = <-promiseChan:
				allPromiseMessages = append(promiseMessages, promiseMessage)
				if *promiseMessage.Ack {
					promiseMessages = append(promiseMessages, promiseMessage)
				}
			case <-tick:
				tickTimeout--
			}
		}

		maxId := int32(-1)
		maxValue := int32(0)
		for _, promiseMessage = range promiseMessages {
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

		for _, conn := range connections {
			go request(acceptChan, conn, serializedRequestMessage, config)
		}

		restart = true
		acceptCount := 0
		for _, _ = range promiseMessages {
			acceptMessage = <-acceptChan
			if *acceptMessage.Ack {
				acceptCount++
				if acceptCount >= quorum {
					restart = false
					break
				}
			}
		}

		if !restart {
			break
		}
	}
	log.Printf("Finally leader elected! And is: %+v\n", data.Leader)
}

func prepare(promiseChan chan message.Promise, conn net.Conn, serializedPrepareMessage []byte, config Config) {
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
	promiseChan <- *promiseMessage.Promise
}

func request(acceptChan chan message.Accept, conn net.Conn, serializedRequestMessage []byte, config Config) {
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
	acceptChan <- *acceptMessage.Accept
}

func handleCoordinationMessage(conn net.Conn) {
	for {
		readBuf := make([]byte, BufSize)
		_, err := conn.Read(readBuf)
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
			go promise(conn, *coordinationMessage.Prepare)
		case message.Message_REQUEST:
			go accept(conn, *coordinationMessage.Request)
		}
	}
}

func promise(conn net.Conn, prepareMessage message.Prepare) {
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
	conn.Write(serializedPromiseMessage)
}

func accept(conn net.Conn, requestMessage message.Request) {
	var acceptMessage *message.Message
	if data.AmITheLeader {
		acceptMessage = &message.Message{
			Type: message.Message_ACCEPT.Enum(),
			Accept: &message.Accept{
				Ack: proto.Bool(false),
			},
		}
	} else if *requestMessage.Prepare.Id == lastAccepted.Id && *requestMessage.Prepare.ProposerId == lastAccepted.ProposerId {
		acceptMessage = &message.Message{
			Type: message.Message_ACCEPT.Enum(),
			Accept: &message.Accept{
				Ack: proto.Bool(true),
			},
		}
		data.Leader = lastAccepted.ProposerId
		data.AmITheLeader = false
		lastAccepted.Value = *requestMessage.Value
		log.Printf("Accepting leadership of %+v\n", data.Leader)
	} else {
		acceptMessage = &message.Message{
			Type: message.Message_ACCEPT.Enum(),
			Accept: &message.Accept{
				Ack: proto.Bool(false),
			},
		}
	}
	serializedAcceptMessage, err := proto.Marshal(acceptMessage)
	if err != nil {
		log.Println("Marshaling error: ", err)
		return
	}
	conn.Write(serializedAcceptMessage)
}

func handleClientMessage(conn net.Conn) {
	readBuf := make([]byte, BufSize)
	conn.Read(readBuf)
	fmt.Printf("read: %+v\n", string(readBuf))
}
