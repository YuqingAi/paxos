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
	opr string
	key string
	value string
}

var Lock sync.Mutex
var Data map[string]string
var IP []string
var Port string
var Timeout time.Duration
var seq_next int //next sequence number, initially 1
var seq_done int //largest sequence number that is done initially 0
var myPaxos *paxos.Paxos

func Index(w http.ResponseWriter, r *http.Request) {
	res := "Hello"
	fmt.Fprint(w, res)
}

func Do_others_opr(mySeq int, Opr Operation) {
	for {
		if mySeq==seq_done+1 {
			Lock.Lock()
			seq_done++
			if Opr.opr=="insert" {
				_, existV2 := Data[Opr.key]
				if (!existV2) {
					Data[Opr.key] = Opr.value
				}
			}
			if Opr.opr=="delete" {
				_, existV2 := Data[Opr.key]
				if (existV2) {
					delete(Data, Opr.key)
				}
			}
			if Opr.opr=="update" {
				_, existV2 := Data[Opr.key]
				if (existV2) {
					Data[Opr.key] = Opr.value
				}
			}
			myPaxos.Done(mySeq)
			Lock.Unlock()
			break
		}
	}
}

func Insert(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Insert!!")
	var tmp StatusFmt
	fmt.Println(r.Method)
	if (r.Method != "POST") {
		tmp.Success = false
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

	query, err := ioutil.ReadAll(r.Body)
	fmt.Println("02")
	if (err != nil) {
		tmp.Success = false
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}
	fmt.Println("03")
	opr, err := url.ParseQuery(string(query))
	keys, existK := opr["key"]
	values, existV := opr["value"]

	if (err != nil || !existK || !existV || len(keys) != 1 || len(values) != 1) {
		tmp.Success = false
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}
	fmt.Println("04")
	k := keys[0]
	v := values[0]

	Lock.Lock()
	seq_next++
	mySeq := seq_next
	Lock.Unlock()
	myOpr := Operation{"insert", k, v}

	myPaxos.Start(mySeq, myOpr)
	var decide bool
	var result Operation
	var res interface{}
	decide = false
	to := 10 * time.Millisecond
fmt.Println("0")
	for decide!=true {
		decide, res = myPaxos.Status(mySeq)
		result = res.(Operation)
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
fmt.Println("1")
	for result!=myOpr {
		go Do_others_opr(mySeq, result)
		Lock.Lock()
		seq_next++
		mySeq = seq_next
		Lock.Unlock()
		myPaxos.Start(mySeq, myOpr)

		decide = false
		decide = false
		to := 10 * time.Millisecond
		for decide!=true {
			decide, res = myPaxos.Status(mySeq)
			result = res.(Operation)
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
	}
fmt.Println("2")
	for {
		if mySeq==seq_done+1 {
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
fmt.Println("3")
	myPaxos.Done(mySeq)
	ret, _ := json.Marshal(tmp)
	fmt.Fprint(w, string(ret))
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

	Lock.Lock()
	seq_next++
	mySeq := seq_next
	Lock.Unlock()
	myOpr := Operation{"delete", k, ""}

	myPaxos.Start(mySeq, myOpr)
	var decide bool
	var result Operation
	var res interface{}
	decide = false
	to := 10 * time.Millisecond
	for decide!=true {
		decide, res = myPaxos.Status(mySeq)
		result = res.(Operation)
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}

	for result!=myOpr {
		go Do_others_opr(mySeq, result)
		Lock.Lock()
		seq_next++
		mySeq = seq_next
		Lock.Unlock()
		myPaxos.Start(mySeq, myOpr)

		decide = false
		to := 10 * time.Millisecond
		for decide!=true {
			decide, res = myPaxos.Status(mySeq)
			result = res.(Operation)
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
	}

	for {
		if mySeq==seq_done+1 {
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
	fmt.Fprint(w, string(ret))
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

	Lock.Lock()
	seq_next++
	mySeq := seq_next
	Lock.Unlock()
	myOpr := Operation{"get", k, ""}

	myPaxos.Start(mySeq, myOpr)
	var decide bool
	var result Operation
	var res interface{}
	decide = false
	to := 10 * time.Millisecond
	for decide!=true {
		decide, res = myPaxos.Status(mySeq)
		result = res.(Operation)
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}

	for result!=myOpr {
		go Do_others_opr(mySeq, result)
		Lock.Lock()
		seq_next++
		mySeq = seq_next
		Lock.Unlock()
		myPaxos.Start(mySeq, myOpr)

		decide = false
		to := 10 * time.Millisecond
		for decide!=true {
			decide, res = myPaxos.Status(mySeq)
			result = res.(Operation)
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
	}

	for {
		if mySeq==seq_done+1 {
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
	fmt.Fprint(w, string(ret))
}

func Update(w http.ResponseWriter, r *http.Request) {

	var tmp StatusFmt

	if (r.Method != "POST") {
		tmp.Success = false
		res, _ := json.Marshal(tmp)
		fmt.Fprint(w, string(res))
		return
	}

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
	seq_next++
	mySeq := seq_next
	Lock.Unlock()
	myOpr := Operation{"update", k, v}

	myPaxos.Start(mySeq, myOpr)
	var decide bool
	var result Operation
	var res interface{}
	decide = false
	to := 10 * time.Millisecond
	for decide!=true {
		decide, res = myPaxos.Status(mySeq)
		result = res.(Operation)
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}

	for result!=myOpr {
		go Do_others_opr(mySeq, result)
		Lock.Lock()
		seq_next++
		mySeq = seq_next
		Lock.Unlock()
		myPaxos.Start(mySeq, myOpr)

		decide = false
		decide = false
		to := 10 * time.Millisecond
		for decide!=true {
			decide, res = myPaxos.Status(mySeq)
			result = res.(Operation)
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
	}

	for {
		if mySeq==seq_done+1 {
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
	fmt.Fprint(w, string(ret))
}

func Countkey(w http.ResponseWriter, r *http.Request) {
	var tmp ResultFmt
	tmp.Result = len(Data)
	res, _ := json.Marshal(tmp)
	fmt.Fprint(w, string(res))
}

func Dump(w http.ResponseWriter, r *http.Request) {
	data := make([][2]string,0)
	for k, v := range Data {
		data = append(data, [2]string{k, v})
	}
	res, _ := json.Marshal(data)
	fmt.Fprint(w, string(res))
}

func Shutdown(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, nil)
	os.Exit(0)
}

func main() {
	arg_num := len(os.Args)
	argv := make([]int, arg_num+1)
	for i := 1 ; i < arg_num; i++{
		argv[i],_ = strconv.Atoi(os.Args[i])
	}

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

	IP[0], exist1 = config["n01"]
	IP[1], exist2 = config["n02"]
	IP[2], exist3 = config["n03"]
	Port, exist4 = config["port"]
	Ports[0], _ = config["p01"]
	Ports[1], _ = config["p02"]
	Ports[2], _ = config["p03"]
	if (!exist1 || !exist2 || !exist3 || !exist4) {
		fmt.Println("Configuration file: Bad format!")
		return
	}

	for i:=0; i<3; i++ {
		IP[i] += ":" + Ports[i]
	}

	Data = make(map[string]string)
	seq_next = 1
	seq_done = 0
	myPaxos = paxos.Make(IP, argv[1], nil)


	fmt.Println("Server " + strconv.Itoa(argv[1]) + " ready")
	http.HandleFunc("/kv/insert", Insert)
	http.HandleFunc("/kv/delete", Delete)
	http.HandleFunc("/kv/get", Get)
	http.HandleFunc("/kv/update", Update)
	http.HandleFunc("/kvman/countkey", Countkey)
	http.HandleFunc("/kvman/dump", Dump)
	http.HandleFunc("/kvman/shutdown", Shutdown)
	http.HandleFunc("/", Index)
	position := ":" + Ports[argv[1]]
	http.ListenAndServe(position, nil)
}