package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	var wg sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		wg.Add(1)

		// Extract worker outside the goroutine for restarting task.
		// Question: why does it fail when a worker is fetched in goroutine?
		worker := <-mr.registerChannel

		// Each task is correlate with i, therefore if the task execute failed,
		// use i = i - 1 can restart the task.
		go func(v int, phase jobPhase, nios int, worker string) {

			defer wg.Done()

			args := DoTaskArgs{
				JobName:       mr.jobName,
				File:          mr.files[v],
				Phase:         phase,
				TaskNumber:    v,
				NumOtherPhase: nios,
			}

			ok := call(worker, "Worker.DoTask", args, new(struct{}))
			if !ok {
				// Restart the task(for mapper, reread mr.files[i).
				i = i - 1
				fmt.Println("call worker error!")
			} else {
				go func() {
					mr.registerChannel <- worker
				}()
			}

		}(i, phase, nios, worker)

	}

	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
