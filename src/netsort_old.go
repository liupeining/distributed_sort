package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

var (
	records_chan = make(chan Record)
	waitGroup    sync.WaitGroup
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

type Record struct {
	Key   [10]byte
	Value [90]byte
}

func connectToServer(address string) net.Conn {
	for {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		fmt.Println("Connected to", address)
		return conn
	}
}

func connectToAllServers(scs ServerConfigs, serverId int) []net.Conn {
	var conns []net.Conn
	for i, server := range scs.Servers {
		if i == serverId {
			continue
		}
		address := net.JoinHostPort(server.Host, server.Port)
		conns = append(conns, connectToServer(address))
	}
	return conns
}

func acceptConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("Could not accept connection : %v", err)
		}
		fmt.Println("Accepted connection from", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	// protocol: 101 bytes, 1 byte: stream_complete + 100 bytes: record
	buffer := make([]byte, 101)
	for {
		_, err := conn.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Error in reading file: %v", err)
		}
		if buffer[0] == 1 {
			break
		}
		var record Record
		copy(record.Key[:], buffer[1:11])
		copy(record.Value[:], buffer[11:])
		records_chan <- record
	}
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := os.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func makeprotocal(buffer []byte, isFinished bool) []byte {
	protocal := make([]byte, 101)
	if isFinished {
		protocal[0] = 1
	} else {
		protocal[0] = 0
		copy(protocal[1:], buffer)
	}
	return protocal
}

func sendRecordsToServer(inputFilePath string, conns []net.Conn) {
	file, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatalf("Error in opening input file - %v", err)
	}
	defer file.Close()

	buffer := make([]byte, 100)
	for {
		_, err := file.Read(buffer)
		if err == io.EOF {
			// sending the signal with the beginning byte as 1
			for _, conn := range conns {
				_, err := conn.Write(makeprotocal(buffer, true))
				if err != nil {
					log.Fatalf("Error in writing to file - %v", err)
				}
			}
			break
		} else if err != nil {
			log.Fatalf("Error in reading file: %v", err)
		}
		// Send the data to the corresponding server, 00 -> server0, 01 -> server1, 10 -> server2 , 11 -> server3
		serverIndex := int(buffer[0]-'0')*2 + int(buffer[1]-'0')
		_, err = conns[serverIndex].Write(makeprotocal(buffer, false))
		if err != nil {
			log.Fatalf("Error in writing to file - %v", err)
		}
	}
}

func startListening(address string) net.Listener {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Could not listen on %s : %v", address, err)
	}
	fmt.Println("Listening on", address)
	return listener
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	/*
		Implement Distributed Sort
	*/

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])

	addr := net.JoinHostPort(scs.Servers[serverId].Host, scs.Servers[serverId].Port)
	listener := startListening(addr)
	defer listener.Close()

	go acceptConnections(listener)
	time.Sleep(1000 * time.Millisecond)

	conns := connectToAllServers(scs, serverId)
	go sendRecordsToServer(os.Args[2], conns)

	// sort records received from other servers
	var records []Record
	for rec := range records_chan {
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].Key[:], records[j].Key[:]) < 0
	})

	output, err := os.Create(os.Args[3])
	if err != nil {
		log.Fatalf("Error in creating output file - %v", err)
	}
	defer output.Close()

	for _, record := range records {
		_, err := output.Write(record.Key[:])
		if err != nil {
			log.Fatalf("Error in writing to file - %v", err)
		}
		_, err = output.Write(record.Value[:])
		if err != nil {
			log.Fatalf("Error in writing to file - %v", err)
		}
	}
}
