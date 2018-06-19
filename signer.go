package main

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	multiHashCount int = 6
	routinesNum    int = 1
)

func toStr(raw interface{}) string {
	switch val := raw.(type) {
	case int:
		return strconv.Itoa(val)
	case string:
		return val
	default:
		panic("Unknown type")
	}
}

func runJob(j job, in, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	done := make(chan bool)

	for i := 0; i < routinesNum; i++ {
		go func(j job, in, out chan interface{}, done chan bool) {
			j(in, out)
			done <- true
		}(j, in, out, done)
	}

	for finishedCount := 0; finishedCount < routinesNum; finishedCount++ {
		<-done
	}
	close(done)
	close(out)
}

// ExecutePipeline Executes pipeline
func ExecutePipeline(jobs ...job) {
	var ins = make([]chan interface{}, len(jobs)+1)
	wg := &sync.WaitGroup{}
	wg.Add(len(jobs))

	for i := range ins {
		ins[i] = make(chan interface{}, 2)
	}

	for i, job := range jobs {
		go runJob(job, ins[i], ins[i+1], wg)
	}

	wg.Wait()
}

// SingleHash Calc single hash
func SingleHash(in, out chan interface{}) {
	var val string
	mu := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	for raw := range in {
		val = toStr(raw)
		go calcSingleHash(val, out, mu, wg)
		wg.Add(1)
		runtime.Gosched()
	}

	wg.Wait()
}

func calcSingleHash(val string, out chan interface{}, mu *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()

	mu.Lock()
	mdfive := DataSignerMd5(val)
	mu.Unlock()

	justCrcResult := calcCrc(val)
	md5CrcResult := calcCrc(mdfive)

	res := <-justCrcResult + "~" + <-md5CrcResult

	fmt.Printf("SingleHash result for %s is %s\n", val, res)

	out <- res
}

// MultiHash Calcs multihash
func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for raw := range in {
		val := toStr(raw)
		go calcMultiHash(val, out, wg)
		wg.Add(1)
		runtime.Gosched()
	}

	wg.Wait()
}

func calcCrc(val string) chan string {
	result := make(chan string)
	go func(val string, result chan string) {
		result <- DataSignerCrc32(val)
		close(result)
	}(val, result)
	return result
}

func calcMultiHash(val string, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	var acc string
	results := make([]chan string, multiHashCount)

	for i := range results {
		results[i] = calcCrc(strconv.Itoa(i) + val)
	}

	for _, result := range results {
		acc += <-result
	}

	fmt.Printf("MultiHash result for %s is %s\n", val, acc)

	out <- acc
}

// CombineResults Combines hashing result
func CombineResults(in, out chan interface{}) {
	acc := make([]string, 0, 100)
	for val := range in {
		acc = append(acc, toStr(val))
		runtime.Gosched()
	}
	sort.Strings(acc)
	res := strings.Join(acc, "_")
	fmt.Printf("Result is %s\n", res)
	out <- res
}

func main() {}
