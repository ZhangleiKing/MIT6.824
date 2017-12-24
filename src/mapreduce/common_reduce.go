package mapreduce

import (
	"encoding/json"
	"io"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	//open File and read keyvalue pair, then sort the array
	kvMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		//get the intermediate file name
		fileName := reduceName(jobName, i, reduceTaskNumber)
		inFile, err := os.Open(fileName)
		if err != nil {
			panic("can't open file: " + fileName)
		}
		//before doReduce close, invoking this
		defer inFile.Close()

		//Read and decode file content
		var kv KeyValue
		//classify key
		for decoder := json.NewDecoder(inFile); decoder.Decode(&kv) != io.EOF; {
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	var keys []string
	//?? should like kvMap.keySet()?
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	//get mergeFileName
	mergeFileName := mergeName(jobName, reduceTaskNumber)

	merge_file, err := os.Create(mergeFileName)
	if err != nil {
		panic("can't create file: " + mergeFileName)
	}
	defer merge_file.Close()

	out_encoder := json.NewEncoder(merge_file)
	for _, k := range keys {
		//statistic each key's frequency sum
		reduced_value := reduceF(k, kvMap[k])
		out_encoder.Encode(KeyValue{k, reduced_value})
	}
}
