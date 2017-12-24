package mapreduce

import (
"encoding/json"
"hash/fnv"
"io/ioutil"
"os"
)

type File_Encoder struct{
	file *os.File
	encoder *json.Encoder
} 

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
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

	//doMap整体流程大致如下：
	//1、打开input文件，读取文件内容
	content, err := ioutil.ReadFile(inFile)
	if err != nil {
		panic("can't read file: " + inFile)
	}

	//2、调用mapF函数，处理文件内容
	kvpairs := mapF(inFile, string(content))

	//3、根据提示，每个file对应一个Encoder，因此定义一个数据结构File_Encoder


	//4、对key进行分类，与中间文件进行映射（相同的key映射到同一文件），然后将keyvalue写入文件

	//构造map<string, File_Encoder>
	open_files := make(map[string]File_Encoder)

	//defer 是延迟函数，里面的内容会在函数return后执行。因为打开的文件需要在当前函数执行完成前进行关闭，因此采用defer
	defer func() {
		for _, file_encoder := range open_files {
			file_encoder.file.Close()
		}
	}()	

	//根据key分类，并将kv写入对应的中间文件
	for _, kv := range kvpairs {
		//构建中间文件名，注意ihasg需要mod nReduce。因为有nReduce个reduce的任务运行，所有的中间文件将会指定给某一个reduce，因此构建中间文件名时，可以通过数字r指定该文件交给哪个reduce任务处理
		ihash_int := int(ihash(kv.Key))
		file_name := reduceName(jobName, mapTaskNumber, ihash_int%nReduce)
		json_encoder := intermediate_file_Encoder(file_name, open_files)
		json_encoder.Encode(&kv)
	}
}

func intermediate_file_Encoder (filename string, open_files map[string]File_Encoder) *json.Encoder {
		file_encoder, ok := open_files[filename]
		if !ok {
			file, err := os.Create(filename)
			if err != nil {
				panic("can't create file:" + filename)
			}
			open_files[filename] = File_Encoder{file, json.NewEncoder(file)}
			return open_files[filename].encoder
		}
		return file_encoder.encoder
}


func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
