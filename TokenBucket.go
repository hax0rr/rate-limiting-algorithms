package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	fmt.Println("STARTED")
	totalThreads = 100

	bucket := BucketConfig{
		Bucket:      make(map[string][]int64),
		Size:        100,
		RefillInSec: 1,
		Lock:        &sync.Mutex{},
	}

	configs := Configs{
		TotalUsers:       1,
		TotalHitsPerUser: 10,
		HitIntervalInMs:  100,
	}
	var wg sync.WaitGroup
	wg.Add(totalThreads)

	for i := 0; i < totalThreads; i++ {
		go bucket.simulateTraffic(&wg, configs, rateLimit)
	}

	wg.Wait()

	fmt.Println("ENDED")
}

type BucketConfig struct {
	Bucket      map[string][]int64 // In-memory bucket implementation
	Size        int64              // Size of bucket
	RefillInSec int64              // Bucket refill interval in deconds
	Lock        *sync.Mutex        // R/W Lock on bucket since we are using in-memory map
}

type Configs struct {
	TotalUsers       int // Total users
	TotalHitsPerUser int // Total calls user will make
	HitIntervalInMs  int // Time interval in ms between each call by a user
}

var totalThreads int // Total threads which will run concurrently; each thread will run for above configuration Configs

func rateLimit(userID, reqID string, now int64, bucket BucketConfig) {
	var res string
	if isAllowed(userID, bucket, now) {
		res = callAPI()
	} else {
		res = render401()
	}

	fmt.Printf("RequestID %s - %s | Time: %s\n", reqID, res, now)
}

func (bucket BucketConfig) simulateTraffic(wg *sync.WaitGroup, config Configs, rateLimit func(userID, reqID string, time int64, bucket BucketConfig)) {
	for i := 0; i < config.TotalUsers; i++ {
		for j := 0; j < config.TotalHitsPerUser; j++ {
			time.Sleep(time.Millisecond * time.Duration(config.HitIntervalInMs))
			reqID := fmt.Sprintf("req-%d-%d", i, j)
			userID := fmt.Sprintf("user-%d", i)

			now := time.Now()

			rateLimit(userID, reqID, now.Unix(), bucket)
		}
	}

	wg.Done()
}

func isAllowed(userID string, bucket BucketConfig, now int64) bool {
	bucket.Lock.Lock()
	if bucket.Bucket[userID] != nil {
		tokens := bucket.Bucket[userID][1]
		timestamp := bucket.Bucket[userID][0]
		if now-timestamp < bucket.RefillInSec {
			if tokens == 0 {
				bucket.Lock.Unlock()
				return false
			}

			tokens--
			bucket.Bucket[userID][1] = tokens
			bucket.Lock.Unlock()
			return true
		}

		bucket.Bucket[userID][0] = now
		bucket.Bucket[userID][1] = bucket.Size - 1
		bucket.Lock.Unlock()
		return true
	}

	bucket.Bucket[userID] = []int64{time.Now().Unix(), bucket.Size - 1}
	bucket.Lock.Unlock()
	return true
}

// Logic to process the request / forward API
func callAPI() string {
	return "Processed"
}

func render401() string {
	return "Too many requests.. please try again later!"
}
