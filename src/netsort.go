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

var connsInProcess int
var mutex sync.Mutex

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

func initListener(serverId int, serverAddress string, scs ServerConfigs) net.Listener {
	listener, err := net.Listen("tcp", serverAddress)
	if err != nil {
		log.Fatalf("Server %d could not listen on %s : %v", serverId, serverAddress, err)
	}
	fmt.Println(">>>>>>>>>>>Server", serverId, "listener initiated")
	return listener
}

func handleConnection(conn net.Conn, wg *sync.WaitGroup) {
	defer conn.Close()
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error in reading data from", conn.RemoteAddr(), err)
			}
			break
		}
		fmt.Printf("Received '%s' from %s\n", string(buffer[:n]), conn.RemoteAddr())
		mutex.Lock()
		connsInProcess--
		mutex.Unlock()
		if connsInProcess == 1 {
			fmt.Println(">>>>>>>>>>>>All connections received")
			wg.Done()
			break
		}
	}
}

func acceptConnection(listener net.Listener, wg *sync.WaitGroup) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("Error in accepting connection %v", err)
		}
		fmt.Println("Accepted connection from", conn.RemoteAddr())
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
		fmt.Println("successfully connected to", address)
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
	connsInProcess = len(scs.Servers)
	fmt.Println("Number of servers:", connsInProcess)

	// step 1: begin listening
	serverAddress := net.JoinHostPort(scs.Servers[serverId].Host, scs.Servers[serverId].Port)
	listener := initListener(serverId, serverAddress, scs)
	defer listener.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go acceptConnection(listener, &wg)

	// step 2: dial other servers
	conns := connectToAllServers(scs, serverId)

	defer func() {
		for _, conn := range conns {
			conn.Close()
		}
	}()

	// step 3: send "hi" to other servers
	for _, conn := range conns {
		_, err := conn.Write([]byte("hi"))
		if err != nil {
			log.Fatalf("Error in writing to connection %v", err)
		}
	}
	wg.Wait()
}
