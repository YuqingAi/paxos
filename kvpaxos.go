package main

import (
	"../paxos"
	"flag"
	"fmt"
	"strconv"
	"net/http"
	"encoding/json"
	"io/ioutil"
	"net/url"
	"sync"
	"time"
)

type StatusFmt struct {
	Success	bool	`json:"success"`
}	

var Lock sync.Mutex
var ServerIP string
var Port string
var Data map[string]string
var Id *int
var Npaxos *int
var Pxa *paxos.Paxos
var Seq int
var Stat int 
var Propose url.Values

func Insert(w http.ResponseWriter, r *http.Request) {	
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
	req := url.Values{}
	req.Set("opr", "ins")
	req.Set("key", k)
	req.Set("value", v)
	req.Set("sender", strconv.Itoa(*Id))
	Lock.Lock()
	Propose = req
	Stat = 1
	for {
		if (Propose == nil) {
			Data[k] = v
			break
		}
	}
	Lock.Unlock()
	return
}	

func solve(r url.Values) {
	if (r.Get("opr") == "ins") {
		kk := r.Get("key")
		vv := r.Get("value")
		_, existV2 :=Data[kk]
		if (!existV2) {
			Data[kk] = vv
		}
	}
}



func waiter() {
	for {
		for {
			done, req2 := Pxa.Status(Seq)
			req3 := req2.(url.Values)
			if (!done) {
				break
			}
			solve(req3)
			if (Stat == 2 && req3.Get("sender") == Propose.Get("sender")) {
				Stat = 0
				Propose = url.Values{}
			}	
			Seq = Seq + 1
		}
		if (Stat == 1) {
			Pxa.Start(Seq, Propose)
			Stat = 2
		}
		time.Sleep(100000000)
	} 
}			 		

func main() {
	Id = flag.Int("id", 0, "id")
	Npaxos = flag.Int("npaxos", 3, "npaxos")
	flag.Parse()
	Propose = url.Values{}
	Seq = 0
	Stat = 0
	ServerIP = "127.0.0.1"
	Port = ":900" + strconv.Itoa(*Id)
	Data = make(map[string]string)
  	var pxh []string = make([]string, *Npaxos)
  	for i := 0; i < *Npaxos; i++ {
    		pxh[i] = ServerIP + ":900" + strconv.Itoa(i)
  	}
  	Pxa = paxos.Make(pxh, *Id, nil)
	fmt.Println("Server" + strconv.Itoa(*Id) + " ready")
	go waiter()
	http.HandleFunc("/kv/insert", Insert)
	position := ":" + Port
	http.ListenAndServe(position, nil)
}
