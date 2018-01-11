package mapreduce

import "fmt"
import "sync"

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	//存储ntask 的状态 0 fail, 1, process, 2 finished
	/*
	nTaskStates := make([]int, ntasks+1)
	// initializes
	for j := range nTaskStates {
		nTaskStates[j] = 0
	}
	fmt.Println("nTaskStates : ***** %v", nTaskStates)

	// index := -1
	// flag :=
	// for true {
  //
	// }
*/
	var wait_group sync.WaitGroup
	// firstly, we have nTasks, and our job is dividing these tasks into the worker by the call function
	for i:=0; i < ntasks; i++ {
		wait_group.Add(1)
		//struct of DoTaskArgs: the information of the job
		var args DoTaskArgs
		args.JobName = jobName
		if phase == mapPhase {
			args.File = mapFiles[i]
		}
		args.Phase = phase
		args.TaskNumber = i
		args.NumOtherPhase = n_other
		reply := ShutdownReply{0}
		// use go routines
		go func ()  {
			defer wait_group.Done()
			// keep runing until success
			for {
				// all the worker is stored in the registerChan channel
				worker := <-registerChan
				//func call(srv string, rpcname string, args interface{}, reply interface{})
				if (call(worker, "Worker.DoTask", &args, &reply)){
					go func(){registerChan <- worker} ()
					break
				}
				/*
				// firstly i want to use this form, but it will go wrong when i want to add the
				// else statement. i still don't know why
				succeeded := call(worker, "Worker.DoTask", &args, &reply)
				if !succeeded {
					fmt.Println("RPC call ERROR *****************")
				}
				else {
					fmt.Println("finished TaskNumber->>>",args.TaskNumber)
				}
				*/
			}
		}()
	}
	// the finish 
	wait_group.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
	// scheduling until all the job are alredy allocated
	/*
	i:=0
	for true {
		// detecting
		flagNumber := 0
		for _, val := range nTaskStates{
			if val == 2 {
				flagNumber++
			}
		}
		//
		if flagNumber == ntasks {
			break
		}

		//for i:=0; i < ntasks; i++ {
		if nTaskStates[i] != 2 {
			wait_group.Add(1)

			//struct of DoTaskArgs: the information of the job
			nTaskStates[i] = 1
			var args DoTaskArgs
			args.JobName = jobName
			if phase == mapPhase {
				args.File = mapFiles[i]
			}
			args.Phase = phase
			args.TaskNumber = i
			args.NumOtherPhase = n_other
			// the job is being scheduled
			//
			if nTaskStates[i] !=2 {
				reply := ShutdownReply{0}
				fmt.Println(i, nTaskStates[i])
				go func ()  {
					defer wait_group.Done()
					worker := <-registerChan
					//func call(srv string, rpcname string,
					//args interface{}, reply interface{})
					succeeded := call(worker, "Worker.DoTask", args, &reply)
					if succeeded {

						//fmt.Println("check len->>>", len(nTaskStates))
						nTaskStates[i] = 2
						fmt.Println("finished TaskNumber->>>",args.TaskNumber)
						i++
					}


					registerChan <- worker
				}()
				//fmt.Println("reply: ", reply.Ntasks)
			}
		}


		//}
	}
	*/





}
