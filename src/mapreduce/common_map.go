package mapreduce
/****
author : CJ
****/
import (
	"fmt"
	"hash/fnv"
    "encoding/json"
    "log"
    "io/ioutil"
    //"strings"
    "os"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	//
	// You will need to write this function.
	//
	// The intermediate output of a map task is stored as multiple
	// files, one per destination reduce task. The file name includes
	// both the map task number and the reduce task number. Use the
	// filename generated by reduceName(jobName, mapTaskNumber, r) as
	// the intermediate file for reduce task r. Call ihash() (see below)
	// on each key, mod nReduce, to pick r for a key/value pair.
	//
	// mapF() is the map function provided by the application. The first
	// argument should be the input file name, though the map function
	// typically ignores it. The second argument should be the entire
	// input file contents. mapF() returns a slice containing the
	// key/value pairs for reduce; see common.go for the definition of
	// KeyValue.
	//
	// Look at Go's ioutil and os packages for functions to read
	// and write files.
	//
	// Coming up with a scheme for how to format the key/value pairs on
	// disk can be tricky, especially when taking into account that both
	// keys and values could contain newlines, quotes, and any other
	// character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	//


	//have a look about the parameters
 	//fmt.Println("Some informations ->>>>> Map: job name = %s, input file = %s, map task id = %d, nReduce = %d\n", jobName, inFile, mapTaskNumber, nReduce)

	bytes, err := ioutil.ReadFile(inFile)
 	if err != nil {
 		log.Fatalf("err: %s", err)
 	}

	kvs := mapF(inFile, string(bytes))


	encoders := make([]*json.Encoder, nReduce);
	for reduceTaskNumber := 0; reduceTaskNumber < nReduce; reduceTaskNumber++ {
		filename := reduceName(jobName, mapTaskNumber, reduceTaskNumber)
		// Create() 默认权限 0666
		file_ptr, err := os.Create(filename)
		if (err != nil) {
			log.Fatal("Unable to create file: ", filename)
		}
		defer file_ptr.Close()
		encoders[reduceTaskNumber] = json.NewEncoder(file_ptr);
	}

	for _, kv := range kvs {
		key := kv.Key
		HashedKey := int(ihash(key) % nReduce)
		//filename := reduceName(jobName, mapTaskNumber, HashedKey)
		//outputFile, _ := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		//enc := json.NewEncoder(outputFile)
		err := encoders[HashedKey].Encode(&kv)
		//err := enc.Encode(&kv)
		//fmt.Println("writing ", key)
		if err != nil {
			fmt.Println(err)
		}
		//outputFile.Close()
	}

	// 这个版本会跑太久*** Test killed: ran too long (10m0s).
/*
	for _, kv := range kvs {
		key := kv.Key
		HashedKey := int(ihash(key) % nReduce)
		filename := reduceName(jobName, mapTaskNumber, HashedKey)
		outputFile, _ := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		enc := json.NewEncoder(outputFile)
		//err := encoders[HashedKey].Encode(&kv)
		err := enc.Encode(&kv)
		fmt.Println("writing ", key)
		if err != nil {
			fmt.Println(err)
		}
		outputFile.Close()
	}

*/
	//outputFile, _ := os.OpenFile("output.txt", os.O_WRONLY|os.O_CREATE, 0666)
    //enc2 := json.NewEncoder(outputFile)
	//var filename = reduceName(jobName, mapTaskNumber, nReduce)
	//var enc =
	//err2 := enc2.Encode(&movie2)
	//if err2 != nil {
		//fmt.Println(err2)
	//}




	//d1 := []byte(jobName)
	//ioutil.WriteFile("output.txt", d1, 0644)
	 /*
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Print(err)
	}
	//string(data)
	*/

/*******
	bytes, err := ioutil.ReadFile(inFile)
	if (err != nil) {
		// log.Fatal() 打印输出并调用 exit(1)
		log.Fatal("Unable to read file: ", inFile)
	}

	// 解析输入文件为 {key,val} 数组
	kv_pairs := mapF(inFile, string(bytes))

	// 生成一组 encoder 用来将 {key,val} 保存至对应文件
	encoders := make([]*json.Encoder, nReduce);
	for reduceTaskNumber := 0; reduceTaskNumber < nReduce; reduceTaskNumber++ {
		filename := reduceName(jobName, mapTaskNumber, reduceTaskNumber)
		// Create() 默认权限 0666
		file_ptr, err := os.Create(filename)
		if (err != nil) {
			log.Fatal("Unable to create file: ", filename)
		}
		// defer 后不能用括号
		defer file_ptr.Close()
		encoders[reduceTaskNumber] = json.NewEncoder(file_ptr);
	}

	// 利用 encoder 将 {key,val} 写入对应的文件
	for _, key_val := range kv_pairs {
		key := key_val.Key
		reduce_idx := ihash(key) % nReduce
		err := encoders[reduce_idx].Encode(key_val)
		if (err != nil) {
			log.Fatal("Unable to write to file")
		}
	}
**********/
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
