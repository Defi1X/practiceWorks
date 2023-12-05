package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

// stack
type Node struct {
	data string
	next *Node
}

type Stack struct {
	Name string
	head *Node
}

func (stack *Stack) pop() (string, error) {
	if stack.head == nil {
		return "", errors.New("Stack is empty")
	} else {
		x := stack.head.data
		stack.head = stack.head.next
		return x, nil
	}
}

func (stack *Stack) push(val string) {
	newNode := &Node{data: val}
	if stack.head == nil {
		stack.head = newNode
		stack.head.data = val
	} else {
		newNode.next = stack.head
		stack.head = newNode
		stack.head.data = val
	}
}

// queue
type Queue struct {
	Name string
	head *Node
	tail *Node
}

func (queue *Queue) push(val string) {
	newNode := &Node{data: val}
	if queue.head == nil {
		queue.head = newNode
		queue.tail = newNode
	} else {
		queue.tail.next = newNode
		queue.tail = newNode
	}
}

func (queue *Queue) pop() (string, error) {
	if queue.head == nil {
		return "", errors.New("Queue is empty!")
	} else {
		data := queue.head.data
		queue.head = queue.head.next
		return data, nil
	}
}

// hashtable
type HashTableNode struct {
	Key   string
	Value string
}

type HashTable struct {
	Name     string
	Table    []*HashTableNode
	capacity int
}

func (ht *HashTable) hashFunc(key string) int {
	hash := 0
	for _, c := range key {
		hash += int(c)
	}
	return hash % ht.capacity
}

func (ht *HashTable) doubleHashFunc(key string) int {
	seed := 31
	hash := 0
	for _, c := range key {
		hash = (hash*seed + int(c)) % ht.capacity
	}
	return hash
}

func NewHashTable(name string, capacity int) *HashTable {
	return &HashTable{
		Name:     name,
		Table:    make([]*HashTableNode, capacity),
		capacity: capacity,
	}
}

func (ht *HashTable) Add(key, value string) {
	entry := &HashTableNode{
		Key:   key,
		Value: value,
	}
	index := ht.hashFunc(key)

	if ht.Table[index] == nil {
		ht.Table[index] = entry
	} else {
		if ht.Table[index].Key == key {
			//rewrite if found that key
			ht.Table[index].Value = value
		} else {
			offset := ht.doubleHashFunc(key)
			for ht.Table[(index+offset)%ht.capacity] != nil {
				if ht.Table[(index+offset)%ht.capacity].Key == key {
					// rewrite if found too
					ht.Table[(index+offset)%ht.capacity].Value = value
					return
				}
				offset = (offset + ht.doubleHashFunc(key)) % ht.capacity
			}
			ht.Table[(index+offset)%ht.capacity] = entry
		}
	}
}

func (ht *HashTable) Get(key string) (string, error) {
	index := ht.hashFunc(key)
	if ht.Table[index] != nil && ht.Table[index].Key == key {
		return ht.Table[index].Value, nil
	} else {
		offset := ht.doubleHashFunc(key)
		for ht.Table[(index+offset)%ht.capacity] != nil && ht.Table[(index+offset)%ht.capacity].Key != key {
			offset = (offset + ht.doubleHashFunc(key)) % ht.capacity
		}
		if ht.Table[(index+offset)%ht.capacity] != nil {
			return ht.Table[(index+offset)%ht.capacity].Value, nil
		}
	}
	return "", errors.New("Key not found")
}

func (ht *HashTable) Delete(key string) (string, error) {
	index := ht.hashFunc(key)

	if ht.Table[index] != nil && ht.Table[index].Key == key {
		ht.Table[index] = nil
		return "Successfully removed", nil
	} else {
		offset := ht.doubleHashFunc(key)
		for ht.Table[(index+offset)%ht.capacity] != nil && ht.Table[(index+offset)%ht.capacity].Key != key {
			offset = (offset + ht.doubleHashFunc(key)) % ht.capacity
		}
		if ht.Table[(index+offset)%ht.capacity] != nil && ht.Table[(index+offset)%ht.capacity].Key == key {
			ht.Table[(index+offset)%ht.capacity] = nil
			return "Successfully removed", nil
		}
	}

	return "", errors.New("Key not found")
}

// set
type Set struct {
	Name string
	ht   *HashTable
}

func NewSet(name string, capacity int) *Set {
	return &Set{Name: name, ht: NewHashTable(name, capacity)}
}

func (set *Set) Add(value string) {
	set.ht.Add(value, value)
}

func (set *Set) IsMember(value string) bool {
	_, err := set.ht.Get(value)
	if err == nil {
		return true
	} else {
		return false
	}
}

func (set *Set) Remove(value string) (string, error) {
	result, err := set.ht.Delete(value)

	if err == nil {
		return result, nil
	} else {
		return "", err
	}

}

type DatabaseStruct struct {
	Name       string
	HashTables []HashTable
	Stacks     []Stack
	Queues     []Queue
	Sets       []Set
}

type MainDatabaseStructure struct {
	databasesList []DatabaseStruct
	mutex         sync.Mutex
}

func (db *DatabaseStruct) dump() {
	fmt.Println("--- HashTables: ")
	for _, table := range db.HashTables {
		fmt.Print(table.Name, ": ")
		for _, element := range table.Table {
			fmt.Print(element, " ")
		}
		fmt.Println()
	}

	fmt.Println("--- Stacks: ")

	for _, st := range db.Stacks {
		fmt.Print(st.Name, ": ")

		some := st

		for {
			result, err := some.pop()
			if err == nil {
				fmt.Print(result, " ")
			} else {
				break
			}
		}
		fmt.Println()
	}
	fmt.Println(db.Stacks)

	fmt.Println("--- Sets: ")

	fmt.Println(db.Sets)

}

var db MainDatabaseStructure

func main() {
	db = MainDatabaseStructure{}
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Something went wrong: ", err)
		return
	}
	defer listener.Close()

	fmt.Println("Server up on 6379")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error on connection: ", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 1024)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed for", conn.LocalAddr())
			} else {
				fmt.Println("Error while reading data: ", err)
			}
			break
		}

		db.mutex.Lock()

		command := string(buffer[:n])

		parts := strings.Split(command, " ")

		// if len(parts) < 5 {
		// 	fmt.Println("incorrecy amount of arguments")
		// 	continue
		// }

		// splitting
		file := parts[0]

		if file == "dump" {
			flagFoundDatabaseWhenDumping := 0
			for _, v := range db.databasesList {
				// fmt.Println("comparing ", v.Name, " with ", parts[1], "result is ", v.Name == parts[1])
				if v.Name == strings.TrimSpace(parts[1]) {
					v.dump()
					flagFoundDatabaseWhenDumping = 1
				}
			}

			if flagFoundDatabaseWhenDumping == 1 {
				continue
			}
		}

		databaseName := strings.TrimSpace(parts[1])
		// query := strings.Trim(parts[2], "\"")

		args := parts[3:]
		// debug
		// fmt.Println("Файл:", file)
		// fmt.Println("Имя базы данных:", databaseName)
		// fmt.Println("Запрос:", query)
		for argumentIndex := range args {
			args[argumentIndex] = strings.TrimSpace(args[argumentIndex])
			args[argumentIndex] = strings.ReplaceAll(args[argumentIndex], "\"", "")
		}
		// fmt.Println("Аргументы:", args, len(args)) // ["HSET, some, key, value"]

		//cleaning

		// args[0] = strings.ReplaceAll(args[0], "\"", "")
		// args[len(args)-1] = strings.ReplaceAll(args[len(args)-1], "\"", "")

		// some more printing
		// for _, arg := range args {
		// 	fmt.Println(arg)
		// }

		// lets go

		foundBase := 0
		baseIndex := -1

		for i := range db.databasesList {
			if db.databasesList[i].Name == databaseName {
				baseIndex = i
				foundBase = 1
			}
		}
		if foundBase == 0 {
			newBase := DatabaseStruct{Name: databaseName}
			db.databasesList = append(db.databasesList, newBase)
			baseIndex = len(db.databasesList) - 1
		}

		action := strings.ToUpper(args[0])

		switch action {
		case "SPUSH":
			foundStruct := 0
			for i := range db.databasesList[baseIndex].Stacks {
				if db.databasesList[baseIndex].Stacks[i].Name == args[1] {
					db.databasesList[baseIndex].Stacks[i].push(args[2])
					foundStruct = 1
				}
			}
			if foundStruct == 0 {
				newStack := Stack{Name: args[1]}
				newStack.push(args[2])
				db.databasesList[baseIndex].Stacks = append(db.databasesList[baseIndex].Stacks, newStack)
			}
		case "SPOP":
			foundStruct := 0
			for i := range db.databasesList[baseIndex].Stacks {

				if db.databasesList[baseIndex].Stacks[i].Name == args[1] {
					result, err := db.databasesList[baseIndex].Stacks[i].pop()
					if err == nil {
						conn.Write([]byte(result + "\n"))
					} else {
						conn.Write([]byte(err.Error() + "\n"))
					}
					foundStruct = 1
				}
			}
			if foundStruct == 0 {
				conn.Write([]byte("Stack doesnt exist" + "\n"))
			}
		case "QPUSH":
			foundStruct := 0
			for i := range db.databasesList[baseIndex].Queues {
				if db.databasesList[baseIndex].Queues[i].Name == args[1] {
					db.databasesList[baseIndex].Queues[i].push(args[2])
					foundStruct = 1
				}
			}
			if foundStruct == 0 {
				newQueue := Queue{Name: args[1]}
				newQueue.push(args[2])
				db.databasesList[baseIndex].Queues = append(db.databasesList[baseIndex].Queues, newQueue)
			}
		case "QPOP":
			foundStruct := 0
			for i := range db.databasesList[baseIndex].Queues {

				if db.databasesList[baseIndex].Queues[i].Name == args[1] {
					result, err := db.databasesList[baseIndex].Queues[i].pop()
					if err == nil {
						conn.Write([]byte(result + "\n"))
					} else {
						conn.Write([]byte(err.Error() + "\n"))
					}
					foundStruct = 1
				}
			}
			if foundStruct == 0 {
				conn.Write([]byte("Queue doesnt exist" + "\n"))
			}
		case "HSET":
			foundStruct := 0
			for i := range db.databasesList[baseIndex].HashTables {
				if db.databasesList[baseIndex].HashTables[i].Name == args[1] {
					db.databasesList[baseIndex].HashTables[i].Add(args[2], args[3])
					foundStruct = 1
				}
			}
			if foundStruct == 0 {
				newTable := NewHashTable(args[1], 512)
				newTable.Add(args[2], args[3])
				db.databasesList[baseIndex].HashTables = append(db.databasesList[baseIndex].HashTables, *newTable)
			}
		case "HGET":
			foundStruct := 0
			for i := range db.databasesList[baseIndex].HashTables {
				if db.databasesList[baseIndex].HashTables[i].Name == args[1] {
					result, err := db.databasesList[baseIndex].HashTables[i].Get(args[2])
					if err == nil {
						conn.Write([]byte(result + "\n"))
					} else {
						conn.Write([]byte(err.Error() + "\n"))
					}
					foundStruct = 1
				}
			}
			if foundStruct == 0 {
				conn.Write([]byte("Hashtable doesnt exist :(" + "\n"))
			}
		case "HDEL":
			foundStruct := 0
			for i := range db.databasesList[baseIndex].HashTables {
				if db.databasesList[baseIndex].HashTables[i].Name == args[1] {
					result, err := db.databasesList[baseIndex].HashTables[i].Delete(args[2])
					if err == nil {
						conn.Write([]byte(result + "\n"))
					} else {
						conn.Write([]byte(err.Error() + "\n"))
					}
					foundStruct = 1
				}
			}
			if foundStruct == 0 {
				conn.Write([]byte("Hashtable doesnt exist :(" + "\n"))
			}
		case "SADD":
			foundStruct := 0
			for i := range db.databasesList[baseIndex].Sets {
				if db.databasesList[baseIndex].Sets[i].Name == args[1] {
					db.databasesList[baseIndex].Sets[i].Add(args[2])
					foundStruct = 1
				}
			}
			if foundStruct == 0 {
				// fmt.Println("Adding new set with name<", args[1], ">")
				newSetVar := NewSet(args[1], 512)
				// fmt.Println(newSetVar)
				newSetVar.Add(args[2])
				db.databasesList[baseIndex].Sets = append(db.databasesList[baseIndex].Sets, *newSetVar)
			}
		case "SREM":
			foundStruct := 0
			for i := range db.databasesList[baseIndex].Sets {
				if db.databasesList[baseIndex].Sets[i].Name == args[1] {
					result, err := db.databasesList[baseIndex].Sets[i].Remove(args[2])
					if err == nil {
						conn.Write([]byte(result + "\n"))
					} else {
						conn.Write([]byte(err.Error() + "\n"))
					}
					foundStruct = 1
				}
			}
			if foundStruct == 0 {
				conn.Write([]byte("Set doesnt exist :(" + "\n"))
			}
		case "SISMEMBER":
			foundStruct := 0
			for i := range db.databasesList[baseIndex].Sets {
				if db.databasesList[baseIndex].Sets[i].Name == args[1] {
					result := db.databasesList[baseIndex].Sets[i].IsMember(args[2])
					conn.Write([]byte(strconv.FormatBool(result) + "\n"))
					foundStruct = 1
				}
			}
			if foundStruct == 0 {
				conn.Write([]byte("Set doesnt exist :(" + "\n"))
			}
		default:
			conn.Write([]byte("Unknown query command" + "\n"))
		}

		db.mutex.Unlock()
	}
}
