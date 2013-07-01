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
    "flag"
	"io/ioutil"
	"net"

	"launchpad.net/goyaml"
)

var BUF_SIZE = 4096

type Flags struct {
    id string
}

type NetAddr struct {
	Host string
	Port int
}

type ReplicaConfig struct {
	ClusterNetAddr NetAddr
	ClientNetAddr NetAddr
}

type Config struct {
	Replicas map[string] ReplicaConfig
}

func main() {
    flags := flagDef()
	config := getConfig("config.yml")
    thisId := flags.id
    thisClusterNetAddr := config.Replicas[thisId].ClusterNetAddr
    thisClientNetAddr := config.Replicas[thisId].ClientNetAddr

	chErrors := make(chan error, 1)
	go startCoordinator(thisClusterNetAddr)
	go startServer(thisClientNetAddr)
    go bootstrap(thisId, config)
	<-chErrors
}

func flagDef() (Flags) {
    // Define the flags that this program can accept.
    var id = flag.String("id", "", "ID of this replica")

    // Parse the flags
    flag.Parse()

    flags := Flags{*id}

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
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			continue
		}
		go handleClientMessage(conn)
	}
}

func bootstrap(thisId string, config Config) {
    for id, replica := config.Replicas {
        conn, err := tcpDial(replica.ClusterNetAddr)
    }
    prepare(thisId)
}

func prepare(thisId string) {

}

func handleCoordinationMessage(conn net.Conn) {
	readBuf := make([]byte, BUF_SIZE)
	conn.Read(readBuf)
	fmt.Printf("read: %+v\n", string(readBuf))
}

func handleClientMessage(conn net.Conn) {
	readBuf := make([]byte, BUF_SIZE)
	conn.Read(readBuf)
	fmt.Printf("read: %+v\n", string(readBuf))
}
