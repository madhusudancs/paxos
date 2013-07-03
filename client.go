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

func main() {
	config := getConfig("config.yml")

	leader := getLeader(config)
	log.Printf("Leader: %s\n", leader)
	if sendData("Some data", leader, config) {
		log.Println("Write success!")
	} else {
		log.Println("Write failed!")
	}

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

func udpDial(netAddr NetAddr) (net.Conn, error) {
	return net.Dial("udp", fmt.Sprintf("%s:%d", netAddr.Host, netAddr.Port))
}

func tcpDial(netAddr NetAddr) (net.Conn, error) {
	return net.Dial("tcp", fmt.Sprintf("%s:%d", netAddr.Host, netAddr.Port))
}

func getLeader(config Config) string {
	timeOut := (config.Heartbeat.Interval * time.Millisecond) * time.Duration(config.Heartbeat.Max)

	getLeaderMessage := &message.GetLeader{}
	serializedGetLeaderMessage, err := proto.Marshal(getLeaderMessage)
	if err != nil {
		log.Println("Marshaling error: ", err)
		return ""
	}

	for id, replica := range config.Replicas {
		conn, err := udpDial(replica.Server.Message)

		if err != nil {
			log.Printf("Dialing to %s failed %+v\n", id, err)
			// handle error
			continue
		}

		conn.SetWriteDeadline(time.Now().Add(timeOut))
		_, err = conn.Write(serializedGetLeaderMessage)
		if err != nil {
			log.Println("GetLeader send failed")
			continue
		}

		readBuf := make([]byte, BufSize)
		conn.SetReadDeadline(time.Now().Add(timeOut))
		_, err = conn.Read(readBuf)
		if err != nil {
			log.Println("Reply Leader receive failed")
			continue
		}
		replyLeaderMessage := &message.ReplyLeader{}
		proto.Unmarshal(readBuf, replyLeaderMessage)
		if replyLeaderMessage.Leader != nil && *replyLeaderMessage.Leader != "" {
			return *replyLeaderMessage.Leader
		}
	}
	return ""
}

func sendData(data string, leader string, config Config) bool {
	conn, err := tcpDial(config.Replicas[leader].Server.Data)
	if err != nil {
		log.Printf("Dialing to %s failed %+v\n", leader, err)
		// handle error
		return false
	}

	clientDataMessage := &message.ClientData{
		Data: []byte(data),
	}
	serializedClientDataMessage, err := proto.Marshal(clientDataMessage)
	if err != nil {
		log.Println("Marshaling error: ", err)
		return false
	}
	_, err = conn.Write(serializedClientDataMessage)
	if err != nil {
		log.Println("Data send failed")
		return false
	}

	readBuf := make([]byte, BufSize)
	_, err = conn.Read(readBuf)
	if err != nil {
		log.Println("Data acknowledgement receive failed")
		return false
	}

	clientDataAckMessage := &message.ClientDataAck{}
	proto.Unmarshal(readBuf, clientDataAckMessage)
	log.Printf("client data ack message: %+v\n", clientDataAckMessage)
	return *clientDataAckMessage.Ack
}
