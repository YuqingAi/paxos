package main

import (
	"net/http"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"sync"
	"time"
	"os"
	"strconv"
	"encoding/gob"
	"../paxos"
)

type StatusFmt struct {
	Success	string	`json:"success"`
}

type ValueFmt struct {
	Success	string	`json:"success"`
	Value	string	`json:value`
}

type ResultFmt struct {
	Result	int	`json:"result"`
}

type Operation struct  {
	Me int
	Opr string
	Key string
	Value string
}

var Lock sync.Mutex
var Data map[string]string
var IP []string
var Port string
var Timeout time.Duration
var seq_next int //next sequence number, initially 1
var seq_done int //largest sequence number that is done initially 0
var myPaxos *paxos.Paxos
var Myid int

func Index(w http.ResponseWriter, r *http.Request) {
	res := "Hello"
	fmt.Fprint(w, res)
}

func Do_others_opr(mySeq int, Opr Operation) {
	for {
		if mySeq==seq_done+1 {
			fmt.Println(mySeq, Opr)
			Lock.Lock()
			seq_done++
			if Opr.Opr=="insert" {
				_, existV2 := Data[Opr.Key]
				if (!existV2) {
					Data[Opr.Key] = Opr.Value
				}
			}
			if Opr.Opr=="delete" {
				_, existV2 := Data[Opr.Key]
				if (existV2) {
					delete(Data, Opr.Key)
				}
			}
			if Opr.Opr=="update" {
				_, existV2 := Data[Opr.Key]
				if (existV2) {
					Data[Opr.Key] = Opr.Value
				}
			}
			myPaxos.Done(mySeq)
			Lock.Unlock()
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func Insert(w http.ResponseWriter, r *http.Request) {
	var tmp StatusFmt
	if (r.Method != "POST") {
		tmp.Success = "false"
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}
	defer r.Body.Close()
	query, err := ioutil.ReadAll(r.Body)
	if (err != nil) {
		tmp.Success = "false"
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}
	opr, err := url.ParseQuery(string(query))
	keys, existK := opr["key"]
	values, existV := opr["value"]

	if (err != nil || !existK || !existV || len(keys) != 1 || len(values) != 1) {
		tmp.Success = "false"
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}
	k := keys[0]
	v := values[0]

	Lock.Lock()
	mySeq := seq_next
	seq_next++
	Lock.Unlock()
	myOpr := Operation{Myid, "insert", k, v}

	myPaxos.Start(mySeq, myOpr)
	var decide bool
	var result Operation
	var res interface{}
	decide = false
	to := 10 * time.Millisecond
	for decide!=true {
		decide, _ = myPaxos.Status(mySeq)
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
	decide, res = myPaxos.Status(mySeq)
	result = res.(Operation)
	for result!=myOpr {
		go Do_others_opr(mySeq, result)
		Lock.Lock()
		mySeq = seq_next
		seq_next++
		Lock.Unlock()
		myPaxos.Start(mySeq, myOpr)

		decide = false
		to := 10 * time.Millisecond
		for decide!=true {
			decide, res = myPaxos.Status(mySeq)
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
		decide, res = myPaxos.Status(mySeq)
		result = res.(Operation)
		time.Sleep(1 * time.Millisecond)
	}

	for {
		if mySeq==seq_done+1 {

			fmt.Println(mySeq, result)
			Lock.Lock()
			seq_done++
			_, existV2 := Data[k]

			if (existV2) {
				tmp.Success = "false"
				Lock.Unlock()
				break
			}
			tmp.Success = "true"
			Data[k] = v
			Lock.Unlock()
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	myPaxos.Done(mySeq)
	ret, _ := json.Marshal(tmp)
	w.Write(ret)
}

func Delete(w http.ResponseWriter, r *http.Request) {
	var tmp ValueFmt
	if (r.Method != "POST") {
		tmp.Success = "false"
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

	query, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if (err != nil) {
		tmp.Success = "false"
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

	opr, err := url.ParseQuery(string(query))
	keys, existK := opr["key"]

	if (err != nil || !existK || len(keys) != 1 ) {
		tmp.Success = "false"
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

	k := keys[0]
	Lock.Lock()
	mySeq := seq_next
	seq_next++
	Lock.Unlock()
	myOpr := Operation{Myid, "delete", k, ""}

	myPaxos.Start(mySeq, myOpr)
	var decide bool
	var result Operation
	var res interface{}
	decide = false
	to := 10 * time.Millisecond
	for decide!=true {
		decide, _ = myPaxos.Status(mySeq)
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
	decide, res = myPaxos.Status(mySeq)
	result = res.(Operation)
	for result!=myOpr {
		go Do_others_opr(mySeq, result)
		Lock.Lock()
		mySeq = seq_next
		seq_next++
		Lock.Unlock()
		myPaxos.Start(mySeq, myOpr)

		decide = false
		to := 10 * time.Millisecond
		for decide!=true {
			decide, res = myPaxos.Status(mySeq)
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
		decide, res = myPaxos.Status(mySeq)
		result = res.(Operation)
		time.Sleep(1 * time.Millisecond)
	}
	for {
		if mySeq==seq_done+1 {

			fmt.Println(mySeq, result)
			Lock.Lock()
			seq_done++
			_, existV2 := Data[k]

			if (!existV2) {
				tmp.Success = "false"
				Lock.Unlock()
				break
			}
			
			tmp.Value = Data[k]
			tmp.Success = "true"
			delete(Data, k)
			Lock.Unlock()
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	myPaxos.Done(mySeq)
	ret, _ := json.Marshal(tmp)
	w.Write(ret)
}


func Get(w http.ResponseWriter, r *http.Request) {

	var tmp ValueFmt
	r.ParseForm()
	if (r.Method != "GET") {
		tmp.Success = "false"
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

	k := r.FormValue("key")
	defer r.Body.Close()
	Lock.Lock()
	mySeq := seq_next
	seq_next++
	Lock.Unlock()
	myOpr := Operation{Myid, "get", k, ""}

	myPaxos.Start(mySeq, myOpr)
	var decide bool
	var result Operation
	var res interface{}
	decide = false
	to := 10 * time.Millisecond
	for decide!=true {
		decide, _ = myPaxos.Status(mySeq)
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
	decide, res = myPaxos.Status(mySeq)
	result = res.(Operation)
	for result!=myOpr {
		go Do_others_opr(mySeq, result)
		Lock.Lock()
		mySeq = seq_next
		seq_next++
		Lock.Unlock()
		myPaxos.Start(mySeq, myOpr)

		decide = false
		to := 10 * time.Millisecond
		for decide!=true {
			decide, res = myPaxos.Status(mySeq)
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
		decide, res = myPaxos.Status(mySeq)
		result = res.(Operation)
		time.Sleep(1 * time.Millisecond)
	}

	for {
		if mySeq==seq_done+1 {
			fmt.Println(mySeq, result)
			Lock.Lock()
			seq_done++
			_, existV2 := Data[k]

			if (!existV2) {
				tmp.Success = "false"
				Lock.Unlock()
				break
			}
			tmp.Success = "true"
			tmp.Value = Data[k]
			Lock.Unlock()
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	myPaxos.Done(mySeq)
	ret, _ := json.Marshal(tmp)
	w.Write(ret)
}

func Update(w http.ResponseWriter, r *http.Request) {

	var tmp StatusFmt

	if (r.Method != "POST") {
		tmp.Success = "false"
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}
	defer r.Body.Close()
	query, err := ioutil.ReadAll(r.Body)

	if (err != nil) {
		tmp.Success = "false"
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

	opr, err := url.ParseQuery(string(query))
	keys, existK := opr["key"]
	values, existV := opr["value"]

	if (err != nil || !existK || !existV || len(keys) != 1 || len(values) != 1) {
		tmp.Success = "false"
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

	k := keys[0]
	v := values[0]

	Lock.Lock()
	mySeq := seq_next
	seq_next++
	Lock.Unlock()
	myOpr := Operation{Myid, "update", k, v}

	myPaxos.Start(mySeq, myOpr)
	var decide bool
	var result Operation
	var res interface{}
	decide = false
	to := 10 * time.Millisecond
	for decide!=true {
		decide, _ = myPaxos.Status(mySeq)
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
	decide, res = myPaxos.Status(mySeq)
	result = res.(Operation)
	for result!=myOpr {
		go Do_others_opr(mySeq, result)
		Lock.Lock()
		mySeq = seq_next
		seq_next++
		Lock.Unlock()
		myPaxos.Start(mySeq, myOpr)

		decide = false
		to := 10 * time.Millisecond
		for decide!=true {
			decide, res = myPaxos.Status(mySeq)
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
		decide, res = myPaxos.Status(mySeq)
		result = res.(Operation)
		time.Sleep(1 * time.Millisecond)
	}

	for {
		if mySeq==seq_done+1 {

			fmt.Println(mySeq, result)
			Lock.Lock()
			seq_done++
			_, existV2 := Data[k]

			if (!existV2) {
				tmp.Success = "false"
				Lock.Unlock()
				break
			}
			tmp.Success = "true"
			Data[k] = v
			Lock.Unlock()
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	myPaxos.Done(mySeq)
	ret, _ := json.Marshal(tmp)
	w.Write(ret)
}

func Countkey(w http.ResponseWriter, r *http.Request) {
	var tmp ResultFmt
	r.ParseForm()
	if (r.Method != "GET") {
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

	Lock.Lock()
	mySeq := seq_next
	seq_next++
	Lock.Unlock()
	myOpr := Operation{Myid, "countkey", "", ""}

	myPaxos.Start(mySeq, myOpr)
	var decide bool
	var result Operation
	var res interface{}
	decide = false
	to := 10 * time.Millisecond
	for decide!=true {
		decide, _ = myPaxos.Status(mySeq)
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
	decide, res = myPaxos.Status(mySeq)
	result = res.(Operation)
	for result!=myOpr {
		go Do_others_opr(mySeq, result)
		Lock.Lock()
		mySeq = seq_next
		seq_next++
		Lock.Unlock()
		myPaxos.Start(mySeq, myOpr)

		decide = false
		to := 10 * time.Millisecond
		for decide!=true {
			decide, res = myPaxos.Status(mySeq)
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
		decide, res = myPaxos.Status(mySeq)
		result = res.(Operation)
		time.Sleep(1 * time.Millisecond)
	}

	for {
		if mySeq==seq_done+1 {
			fmt.Println(mySeq, result)
			Lock.Lock()
			seq_done++
			tmp.Result = len(Data)
			Lock.Unlock()
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	myPaxos.Done(mySeq)
	ret, _ := json.Marshal(tmp)
	w.Write(ret)
}

func Dump(w http.ResponseWriter, r *http.Request) {
	var tmp ValueFmt
	r.ParseForm()
	if (r.Method != "GET") {
		tmp.Success = "false"
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

	Lock.Lock()
	mySeq := seq_next
	seq_next++
	Lock.Unlock() 
	myOpr := Operation{Myid, "dump", "", ""}

	myPaxos.Start(mySeq, myOpr)
	var decide bool
	var result Operation
	var res interface{}
	decide = false
	to := 10 * time.Millisecond
	for decide!=true {
		decide, _ = myPaxos.Status(mySeq)
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
	decide, res = myPaxos.Status(mySeq)
	result = res.(Operation)
	for result!=myOpr {
		go Do_others_opr(mySeq, result)
		Lock.Lock()
		mySeq = seq_next
		seq_next++
		Lock.Unlock()
		myPaxos.Start(mySeq, myOpr)

		decide = false
		to := 10 * time.Millisecond
		for decide!=true {
			decide, res = myPaxos.Status(mySeq)
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
		decide, res = myPaxos.Status(mySeq)
		result = res.(Operation)
		time.Sleep(1 * time.Millisecond)
	}

	data := make([][2]string,0)
	for {
		if mySeq==seq_done+1 {
			fmt.Println(mySeq, result)
			Lock.Lock()
			seq_done++
			for k, v := range Data {
				data = append(data, [2]string{k, v})
			}
			Lock.Unlock()
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	myPaxos.Done(mySeq)
	ret, _ := json.Marshal(data)
	w.Write(ret)
}

func Shutdown(w http.ResponseWriter, r *http.Request) {
	var tmp ValueFmt
	r.ParseForm()
	if (r.Method != "GET") {
		tmp.Success = "false"
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

	Lock.Lock()
	mySeq := seq_next
	seq_next++
	Lock.Unlock()
	myOpr := Operation{Myid, "shutdown", "", ""}

	myPaxos.Start(mySeq, myOpr)
	var decide bool
	var result Operation
	var res interface{}
	decide = false
	to := 10 * time.Millisecond
	for decide!=true {
		decide, _ = myPaxos.Status(mySeq)
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
	decide, res = myPaxos.Status(mySeq)
	result = res.(Operation)
	for result!=myOpr {
		go Do_others_opr(mySeq, result)
		Lock.Lock()
		mySeq = seq_next
		seq_next++
		Lock.Unlock()
		myPaxos.Start(mySeq, myOpr)

		decide = false
		to := 10 * time.Millisecond
		for decide!=true {
			decide, res = myPaxos.Status(mySeq)
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
		decide, res = myPaxos.Status(mySeq)
		result = res.(Operation)
		time.Sleep(1 * time.Millisecond)
	}

	for {
		if mySeq==seq_done+1 {
			Lock.Lock()
			seq_done++
			fmt.Println(mySeq, "Shutdown!")
			Lock.Unlock()
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	myPaxos.Done(mySeq)
	w.Write(nil)
	os.Exit(0)
}

func main() {
	gob.Register(Operation{})
	var str string
	str = os.Args[1]
	Myid, _ = strconv.Atoi(str[1:3])
	Myid = Myid - 1
	var config = map[string]string{}
	Timeout = 5.0 * time.Second
	bytes, err := ioutil.ReadFile("../conf/settings.conf")

	if (err != nil) {
		fmt.Println("Missing configuration file!")
		return
	}

	err = json.Unmarshal(bytes, &config)

	if (err != nil) {
		fmt.Println("Unmarshal: ", err.Error())
		return
	}

	var existp bool

	var IP []string = make([]string, 99)
	var count int
	var exist bool
	count = 1
	exist = false
	var name string
	for {
		if count<10 {
			name = "n0"+strconv.Itoa(count)
		}else {
			name = "n"+strconv.Itoa(count)
		}
		IP[count - 1], exist = config[name]
		if !exist {
			break
		}
		count++
	}
	Port, existp = config["port"]
	if (!existp || count<2) {
		fmt.Println("Configuration file: Bad format!")
		return
	}
	IP = IP[0:count-1]
	
	Data = make(map[string]string)
	seq_next = 1
	seq_done = 0
	
	myPaxos = paxos.Make(IP, Myid, nil)

	fmt.Println("Server ready")
	http.HandleFunc("/kv/insert", Insert)
	http.HandleFunc("/kv/delete", Delete)
	http.HandleFunc("/kv/get", Get)
	http.HandleFunc("/kv/update", Update)
	http.HandleFunc("/kvman/countkey", Countkey)
	http.HandleFunc("/kvman/dump", Dump)
	http.HandleFunc("/kvman/shutdown", Shutdown)
	http.HandleFunc("/", Index)
	position := ":" + Port
	err = http.ListenAndServe(position, nil)
	if (err != nil) {
		fmt.Println("Failed to listen!")
	}
}
