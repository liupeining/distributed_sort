package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"sort"
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
var records []Record
var recordsMutex sync.Mutex

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

func handleConnection(conn net.Conn, wg *sync.WaitGroup, serverId int, nodesCount int) {
	defer conn.Close()
	defer wg.Done()
	for {
		buffer := make([]byte, 0, 101)
		bytesRead := 0
		for bytesRead < 101 {
			buf := make([]byte, 101-bytesRead)
			n, err := conn.Read(buf)
			if err != nil {
				if err != io.EOF {
					fmt.Println("Error in reading data from", conn.RemoteAddr(), err)
				}
				break
			}
			bytesRead += n
			buffer = append(buffer, buf[:n]...)
		}
		if len(buffer) != 101 {
			fmt.Println("Error in reading data from", conn.RemoteAddr(), "expected 101 bytes, got", len(buffer))
			break
		}
		if buffer[0] == 1 {
			break
		} else {
			bufferID := getBufferID(buffer, nodesCount)
			if bufferID != serverId {
				continue
			}
			record := buffer2Record(buffer)
			recordsChan <- record
		}
	}
}

func getBufferID(buffer []byte, nodesCount int) int {
	if nodesCount <= 1 {
		return 0
	}
	bits := int(math.Ceil(math.Log2(float64(nodesCount))))
	mask := (1<<bits - 1) << (8 - bits)
	return int((buffer[1] & byte(mask)) >> (8 - bits))
}

func buffer2Record(buffer []byte) Record {
	var record Record
	copy(record.Key[:], buffer[1:11])
	copy(record.Value[:], buffer[11:])
	return record
}

func acceptConnection(listener net.Listener, wg *sync.WaitGroup, serverId int, nodesCount int) {
	for {
		conn, err := listener.Accept()
		fatalOnError(err, "Could not accept connection")
		go handleConnection(conn, wg, serverId, nodesCount)
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
	recordsMutex.Lock()
	for record := range recordsChan {
		records = append(records, record)
	}
	recordsMutex.Unlock()
}

func connsClose(conns []net.Conn) {
	for _, conn := range conns {
		conn.Close()
	}
}

func sendRecords(inputFile *os.File, conns []net.Conn, serverId int, nodesCount int) {
	buffer := make([]byte, 101)
	for {
		buffer[0] = 0
		_, err := inputFile.Read(buffer[1:])
		if err != nil {
			if err == io.EOF {
				buffer[0] = 1
				for _, conn := range conns {
					_, err := conn.Write(buffer)
					fatalOnError(err, "Error in writing to connection")
				}
				break
			} else {
				fatalOnError(err, "Error in reading input file")
			}
		}
		bufferID := getBufferID(buffer, nodesCount)
		if bufferID == serverId {
			record := buffer2Record(buffer)
			recordsChan <- record
		} else {
			for _, conn := range conns {
				_, err := conn.Write(buffer)
				fatalOnError(err, "Error in writing to connection")
			}
		}
	}
}

func sortRecordsAndSave(outputFilePath string) {
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].Key[:], records[j].Key[:]) < 0
	})
	output, err := os.Create(outputFilePath)
	fatalOnError(err, fmt.Sprintf("Error in creating output file %s", outputFilePath))
	defer output.Close()
	for _, record := range records {
		_, err := output.Write(record.Key[:])
		fatalOnError(err, "Error in writing to file")
		_, err = output.Write(record.Value[:])
		fatalOnError(err, "Error in writing to file")
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
	var wg sync.WaitGroup
	go processRecords()
	nodesCount := len(scs.Servers)

	// step 1: begin listening
	serverAddress := net.JoinHostPort(scs.Servers[serverId].Host, scs.Servers[serverId].Port)
	listener := initListener(serverId, serverAddress, scs)
	defer listener.Close()
	wg.Add(nodesCount - 1)
	go acceptConnection(listener, &wg, serverId, nodesCount)

	// step 2: dial other servers
	conns := connectToAllServers(scs, serverId)
	defer connsClose(conns)

	// step 3: send records to other servers
	inputFile := openInputFile(os.Args[2])
	defer inputFile.Close()
	sendRecords(inputFile, conns, serverId, nodesCount)

	wg.Wait()
	defer close(recordsChan)
	time.Sleep(1000 * time.Millisecond)

	// step 4: sort records received from other servers
	sortRecordsAndSave(os.Args[3])
	log.Printf("Sorting %s to %s\n", os.Args[1], os.Args[2])
}
