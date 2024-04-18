package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

type Record struct {
	Key   [10]byte
	Value [90]byte
}

var recordsChan = make(chan Record)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
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

func fatalOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func initListener(serverId int, serverAddress string, scs ServerConfigs) net.Listener {
	listener, err := net.Listen("tcp", serverAddress)
	fatalOnError(err, fmt.Sprintf("Server %d could not listen on %s", serverId, serverAddress))
	return listener
}

func handleConnection(conn net.Conn, wg *sync.WaitGroup) {
	defer conn.Close()
	defer wg.Done()
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error in reading data from", conn.RemoteAddr(), err)
			}
			break
		}
		fmt.Println("Received", n, "bytes from", conn.RemoteAddr())
		if buffer[0] == 1 {
			break
		} else {
			var record Record
			copy(record.Key[:], buffer[1:11])
			copy(record.Value[:], buffer[11:])
			fmt.Println("Received record", record)
			recordsChan <- record

			break // temporary break
		}
	}
}

func acceptConnection(listener net.Listener, wg *sync.WaitGroup) {
	for {
		conn, err := listener.Accept()
		fatalOnError(err, "Could not accept connection")
		go handleConnection(conn, wg)
	}
}

func connectToServer(address string) net.Conn {
	for {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			time.Sleep(250 * time.Millisecond)
			continue
		}
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

func openInputFile(inputFilePath string) *os.File {
	file, err := os.Open(inputFilePath)
	fatalOnError(err, fmt.Sprintf("Error in opening input file %s", inputFilePath))
	return file
}

func processRecords() {
	for record := range recordsChan {
		fmt.Println("Processed record:", record)
	}
}

func connsClose(conns []net.Conn) {
	for _, conn := range conns {
		conn.Close()
	}
}

func sendRecords(inputFile *os.File, conns []net.Conn) {
	//(for now, just the first record in input file)
	buffer := make([]byte, 101)
	buffer[0] = 0
	_, err := inputFile.Read(buffer[1:])
	fatalOnError(err, "Error in reading input file")

	for _, conn := range conns {
		_, err := conn.Write(buffer)
		fatalOnError(err, "Error in writing to connection")
	}
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

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort
	*/

	go processRecords()

	// step 1: begin listening
	serverAddress := net.JoinHostPort(scs.Servers[serverId].Host, scs.Servers[serverId].Port)
	listener := initListener(serverId, serverAddress, scs)
	defer listener.Close()
	var wg sync.WaitGroup
	wg.Add(len(scs.Servers) - 1)
	go acceptConnection(listener, &wg)

	// step 2: dial other servers
	conns := connectToAllServers(scs, serverId)
	defer connsClose(conns)

	// step 3: send a record to other servers
	inputFile := openInputFile(os.Args[2])
	defer inputFile.Close()
	sendRecords(inputFile, conns)

	wg.Wait()
	close(recordsChan)
}
