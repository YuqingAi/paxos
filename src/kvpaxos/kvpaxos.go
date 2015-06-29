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
	"paxos"
	"encoding/gob"
)

type StatusFmt struct {
	Success	bool	`json:"success"`
}

type ValueFmt struct {
	Success	bool	`json:"success"`
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
	}
}

func Insert(w http.ResponseWriter, r *http.Request) {
	var tmp StatusFmt
	if (r.Method != "POST") {
		tmp.Success = false
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}
	defer r.Body.Close()
	query, err := ioutil.ReadAll(r.Body)
	if (err != nil) {
		tmp.Success = false
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}
	opr, err := url.ParseQuery(string(query))
	keys, existK := opr["key"]
	values, existV := opr["value"]

	if (err != nil || !existK || !existV || len(keys) != 1 || len(values) != 1) {
		tmp.Success = false
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
	}

	for {
		if mySeq==seq_done+1 {

			fmt.Println(mySeq)
			Lock.Lock()
			seq_done++
			_, existV2 := Data[k]

			if (existV2) {
				tmp.Success = false
				Lock.Unlock()
				break
			}
			tmp.Success = true
			Data[k] = v
			Lock.Unlock()
			break
		}
	}
	myPaxos.Done(mySeq)
	ret, _ := json.Marshal(tmp)
	w.Write(ret)
}

func Delete(w http.ResponseWriter, r *http.Request) {
	var tmp StatusFmt
	if (r.Method != "POST") {
		tmp.Success = false
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

	query, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if (err != nil) {
		tmp.Success = false
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

	opr, err := url.ParseQuery(string(query))
	keys, existK := opr["key"]

	if (err != nil || !existK || len(keys) != 1 ) {
		tmp.Success = false
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
	}
	for {
		if mySeq==seq_done+1 {

			fmt.Println(mySeq)
			Lock.Lock()
			seq_done++
			_, existV2 := Data[k]

			if (!existV2) {
				tmp.Success = false
				Lock.Unlock()
				break
			}
			tmp.Success = true
			delete(Data, k)
			Lock.Unlock()
			break
		}
	}
	myPaxos.Done(mySeq)
	ret, _ := json.Marshal(tmp)
	w.Write(ret)
}


func Get(w http.ResponseWriter, r *http.Request) {

	var tmp ValueFmt
	r.ParseForm()
	if (r.Method != "GET") {
		tmp.Success = false
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
	}

	for {
		if mySeq==seq_done+1 {
			fmt.Println(mySeq)
			Lock.Lock()
			seq_done++
			_, existV2 := Data[k]

			if (!existV2) {
				tmp.Success = false
				Lock.Unlock()
				break
			}
			tmp.Success = true
			tmp.Value = Data[k]
			Lock.Unlock()
			break
		}
	}
	myPaxos.Done(mySeq)
	ret, _ := json.Marshal(tmp)
	w.Write(ret)
}

func Update(w http.ResponseWriter, r *http.Request) {

	var tmp StatusFmt

	if (r.Method != "POST") {
		tmp.Success = false
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}
	defer r.Body.Close()
	query, err := ioutil.ReadAll(r.Body)

	if (err != nil) {
		tmp.Success = false
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

	opr, err := url.ParseQuery(string(query))
	keys, existK := opr["key"]
	values, existV := opr["value"]

	if (err != nil || !existK || !existV || len(keys) != 1 || len(values) != 1) {
		tmp.Success = false
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
	}

	for {
		if mySeq==seq_done+1 {

			fmt.Println(mySeq)
			Lock.Lock()
			seq_done++
			_, existV2 := Data[k]

			if (!existV2) {
				tmp.Success = false
				Lock.Unlock()
				break
			}
			tmp.Success = true
			Data[k] = v
			Lock.Unlock()
			break
		}
	}
	myPaxos.Done(mySeq)
	ret, _ := json.Marshal(tmp)
	w.Write(ret)
}

func Countkey(w http.ResponseWriter, r *http.Request) {
	var tmp ResultFmt
	Lock.Lock()
	tmp.Result = len(Data)
	Lock.Unlock()
	res, _ := json.Marshal(tmp)
	w.Write(res)
}

func Dump(w http.ResponseWriter, r *http.Request) {
	data := make([][2]string,0)
	Lock.Lock()
	for k, v := range Data {
		data = append(data, [2]string{k, v})
	}
	Lock.Unlock()
	res, _ := json.Marshal(data)
	w.Write(res)
}

func Shutdown(w http.ResponseWriter, r *http.Request) {
	w.Write(nil)
	os.Exit(0)
}

func main() {
	gob.Register(Operation{})
	arg_num := len(os.Args)
	argv := make([]int, arg_num+1)
	for i := 1 ; i < arg_num; i++{
		argv[i],_ = strconv.Atoi(os.Args[i])
	}
	Myid = argv[1]
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

	var exist1 bool
	var exist2 bool
	var exist3 bool
	var exist4 bool

	var IP []string = make([]string, 3)
	var Ports []string = make([]string, 3)
	var Ports2 []string = make([]string, 3)
	IP[0], exist1 = config["n01"]
	IP[1], exist2 = config["n02"]
	IP[2], exist3 = config["n03"]
	Port, exist4 = config["port"]
	Ports[0], _ = config["p01"]
	Ports[1], _ = config["p02"]
	Ports[2], _ = config["p03"]
	Ports2[0], _ = config["p11"]
	Ports2[1], _ = config["p12"]
	Ports2[2], _ = config["p13"]

	if (!exist1 || !exist2 || !exist3 || !exist4) {
		fmt.Println("Configuration file: Bad format!")
		return
	}

	for i:=0; i<3; i++ {
		IP[i] += ":" + Ports2[i]
	}

	Data = make(map[string]string)
	seq_next = 1
	seq_done = 0
	myPaxos = paxos.Make(IP, Myid, nil)

	fmt.Println("Server " + strconv.Itoa(argv[1]) + " ready")
	http.HandleFunc("/kv/insert", Insert)
	http.HandleFunc("/kv/delete", Delete)
	http.HandleFunc("/kv/get", Get)
	http.HandleFunc("/kv/update", Update)
	http.HandleFunc("/kvman/countkey", Countkey)
	http.HandleFunc("/kvman/dump", Dump)
	http.HandleFunc("/kvman/shutdown", Shutdown)
	http.HandleFunc("/", Index)
	position := ":" + Ports[Myid]
	http.ListenAndServe(position, nil)
}
