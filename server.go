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

type Data struct {
	leader string
}

var data Data

func main() {
	flags := flagDef()
	config := getConfig("config.yml")
	thisId := flags.id

	if _, ok := config.Replicas[thisId]; !ok {
		panic(fmt.Sprintf("No configuration defined for ID: \"%s\"\n", thisId))
	}

	thisClusterNetAddr := config.Replicas[thisId].Cluster
	thisClientNetAddr := config.Replicas[thisId].Client

	chErrors := make(chan error, 1)
	go startCoordinator(thisClusterNetAddr)
	go startServer(thisClientNetAddr)
	go bootstrap(flags, config)
	<-chErrors
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

func bootstrap(flags Flags, config Config) {
	numReplicas := len(config.Replicas)
	quorom := (numReplicas / 2) + 1
	prepare_id := int32(-1)
	thisId := flags.id
	connections := make(map[string]net.Conn)

	accept := make(chan Accept, numReplicas)

	log.Println("Bootstrapping ...")

	tick := time.Tick(config.Heartbeat.Interval * time.Millisecond)
	for _ = range tick {
		if data.leader != "" {
			break
		}
		for id, replica := range config.Replicas {
			if _, ok := connections[id]; ok || id == thisId {
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
		prepareMessage := &message.Prepare{
			ProposerId: proto.String(thisId),
			Id:         proto.Int32(prepare_id),
		}
		serializedPrepareMessage, err := proto.Marshal(prepareMessage)
		if err != nil {
			log.Fatal("Marshaling error: ", err)
		}
		for _, conn := range connections {
			go prepare(conn, serializedPrepareMessage)
		}

	}
}

func prepare(conn net.Conn, serializedPrepareMessage []byte) {
	conn.Write(serializedPrepareMessage)
}

func handleCoordinationMessage(conn net.Conn) {
	readBuf := make([]byte, BufSize)
	conn.Read(readBuf)
	fmt.Printf("read: %+v\n", string(readBuf))
}

func handleClientMessage(conn net.Conn) {
	readBuf := make([]byte, BufSize)
	conn.Read(readBuf)
	fmt.Printf("read: %+v\n", string(readBuf))
}
