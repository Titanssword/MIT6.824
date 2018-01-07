package mapreduce
/****
author : CJ
****/
import (
	"fmt"
	"os"
	"encoding/json"
	"sort"
	"log"
	"io"
)
// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	//使用map 存储key to key对应的多个值的数组
	kvMap := make(map[string]([]string))

	for mapNumber := 0; mapNumber < nMap; mapNumber++ {
		filename := reduceName(jobName, mapNumber, reduceTaskNumber)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("err in open  file: %s", err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		for {
	        var kv KeyValue
	        if err := decoder.Decode(&kv); err == io.EOF {
	  			break
	  		} else if err != nil {
	  			log.Fatal(err)
	  		}
	        //map append 方法
	        kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)

	    }
	}

	keys := make([]string, 0, len(kvMap))
	for k, _ := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	newFile , err2:= os.Create(mergeName(jobName, reduceTaskNumber))
	if err2 != nil {
		fmt.Println("reduce merge files: %s cant open ", mergeName(jobName, reduceTaskNumber))
		return
	}
	enc := json.NewEncoder(newFile)
	for _, k := range keys {
		enc.Encode(KeyValue{k, reduceF(k,  kvMap[k])})
		//fmt.Println("reduceF results is  %s")
		//fmt.Println(reduceF(k,  kvMap[k]))
	}
	defer newFile.Close()
}
