package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pomozoff/bigqueue"
)

func main() {
	var queue = new(bigqueue.FileQueue)

	// use custom options
	var DefaultOptions = &bigqueue.Options{
		DataPageSize:      bigqueue.DefaultDataPageSize,
		IndexItemsPerPage: bigqueue.DefaultIndexItemsPerPage,
	}

	// open queue files
	err := queue.Open("./bin", "testqueue", DefaultOptions)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	for i := 1; i < 10; i++ {
		data := []byte("hello jhunters" + strconv.Itoa(i))
		i, err := queue.Enqueue(data)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Enqueued index=", i, string(data))
		}
	}

	queue.Subscribe(func(index int64, bb []byte, err error) {
		fmt.Println("index=", index, " value=", string(bb))
	})

	//	for y := 0; y < 10; y++ {
	//		// do enqueue
	//		for i := 1; i < 10; i++ {
	//			data := []byte("hello jhunters" + strconv.Itoa(i))
	//			i, err := queue.Enqueue(data)
	//			if err != nil {
	//				fmt.Println(err)
	//			} else {
	//				fmt.Println("Enqueued index=", i, string(data))
	//			}
	//		}
	//		time.Sleep(time.Duration(1) * time.Second)
	//	}

	time.Sleep(time.Duration(10) * time.Second)
}
