package main

import (
	"fmt"
	"io/ioutil"
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sort"
	"time"
	"strings"
	"math/rand"
)

var ServerPort string
var ServerIntPort int
var NumPeers int
var Peers []string

var PrimaryIP, Port string
var lock sync.Mutex
var valuelen int=20
var totalinsert,successinsert int=0,0

type intSlice []int64

var inslis, getlis =intSlice{},intSlice{}

type myjson1 struct {
	Success	string	`json:"success"`
}

type myjson2 struct {
	Success	string	`json:"success"`
	Value	string	`json:value`
}

type myjson3 struct {
	Result	int	`json:"result"`
}

func (lis intSlice) Len() int {
	return len(lis)
}

func (lis intSlice) Less(i, j int) bool {
	if lis[i] < lis[j] {
		return true
	}

	return false
}

func (lis intSlice) Swap(i, j int) {
	var temp int64 = lis[i]
	lis[i] = lis[j]
	lis[j] = temp
}

func Insert(key string, value string) (ret bool) {

	lock.Lock()
	totalinsert++
	lock.Unlock()

	var starttime time.Time=time.Now()

	defer func() {
		var endtime time.Time=time.Now()
		var timelength int64=endtime.Sub(starttime).Nanoseconds()
		lock.Lock()
		inslis=append(inslis,timelength)
		lock.Unlock()
		if r := recover(); r != nil {
			fmt.Println("Server not responding!")
			ret=false
		}

		if ret==true{
			lock.Lock()
			successinsert++
			lock.Unlock()
		}
	}()

	data := url.Values{"key":{key}, "value":{value}}
	req, _ := http.Post("http://"+PrimaryIP+":"+Port+"/kv/insert", "application/x-www-form-urlencoded", bytes.NewBufferString(data.Encode()))
	defer req.Body.Close()

	res, _ := ioutil.ReadAll(req.Body)
	fmt.Println(string(res))

	var resp myjson1
	err := json.Unmarshal(res, &resp)
	if err != nil {
		fmt.Println("Invalid response!!")
		return false
	}

	result:=resp.Success;

	if(result!="true"){
		fmt.Println("Operation failed!")
		return false
	}

	return true
}

func Delete(key string) (ret bool, value string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Server not responding!")
			ret=false
		}
	}()
	data := url.Values{"key":{key}}
	req, _ := http.Post("http://"+PrimaryIP+":"+Port+"/kv/delete", "application/x-www-form-urlencoded", bytes.NewBufferString(data.Encode()))
	defer req.Body.Close()
	res, _ := ioutil.ReadAll(req.Body)
	fmt.Println(string(res))

	var resp myjson2

	err := json.Unmarshal(res, &resp)
	if(err!=nil) {
		fmt.Println("Invalid response!")
		return false,""
	}

	result:=resp.Success;

	if(result!="true"){
		fmt.Println("Operation failed!")
		return false,""
	}

	return true,resp.Value
}

func Get(key string) (ret bool, value string){

	var starttime time.Time=time.Now()
	defer func() {
		var endtime time.Time=time.Now()
		var timelength int64=endtime.Sub(starttime).Nanoseconds()
		lock.Lock()
		getlis=append(getlis,timelength)
		lock.Unlock()
		if r := recover(); r != nil {
			fmt.Println("Server not responding!")
			ret=false
		}
	}()

	tmp:=url.Values{}
	tmp.Set("key", key)
	//fmt.Println("http://"+PrimaryIP+":"+Port+"/kv/get?"+tmp.Encode())
	req, _ := http.Get("http://"+PrimaryIP+":"+Port+"/kv/get?"+tmp.Encode())
	defer req.Body.Close()
	res, _ := ioutil.ReadAll(req.Body)
	fmt.Println(string(res))

	var resp myjson2
	err := json.Unmarshal(res, &resp)
	if(err!=nil) {
		fmt.Println("Invalid response!")
		return false,""
	}

	result:=resp.Success;

	if(result!="true"){
		fmt.Println("Operation failed!")
		return false,""
	}

	return true,resp.Value
}

func Update(key string, value string) (ret bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Server not responding!")
			ret=false
		}
	}()
	data := url.Values{"key":{key}, "value":{value}}
	req, _ := http.Post("http://"+PrimaryIP+":"+Port+"/kv/update", "application/x-www-form-urlencoded", bytes.NewBufferString(data.Encode()))
	defer req.Body.Close()
	res, _ := ioutil.ReadAll(req.Body)
	fmt.Println(string(res))

	var resp myjson1
	err := json.Unmarshal(res, &resp)
	if err != nil {
		fmt.Println("Invalid response!!")
		return false
	}

	result:=resp.Success;

	if(result!="true"){
		fmt.Println("Operation failed!")
		return false
	}

	return true
}

func Dump(serverIP string) (ret bool, dumpresult map[string]string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Server not responding!")
			ret =false
		}
	}()
	//fmt.Println("http://"+serverIP+":"+Port+"/kvman/dump")
	req, _ := http.Get("http://"+serverIP+":"+Port+"/kvman/dump")
	defer req.Body.Close()
	res, _ := ioutil.ReadAll(req.Body)
	fmt.Println(string(res))

	var resp []([2]string)
	err := json.Unmarshal(res, &resp)
	if err != nil {
		//fmt.Println(err)
		fmt.Println("Invalid response!!")
		return false, nil
	}

	dumpresult=make(map[string]string)
	for i:=0;i<len(resp);i++{
		dumpresult[resp[i][0]]=resp[i][1]
	}

	return true,dumpresult
}

func Shutdown(serverIP string) (ret bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Server not responding!")
			ret=false
		}
	}()
	http.Get("http://"+serverIP+":"+Port+"/kvman/shutdown")
	return true
}

func Countkey(serverIP string) (count int){
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Server not responding!")
			count=-1
		}
	}()
	req, _ := http.Get("http://"+serverIP+":"+Port+"/kvman/countkey")
	defer req.Body.Close()
	res, _ := ioutil.ReadAll(req.Body)
	fmt.Println(string(res))

	var resp myjson3
	err := json.Unmarshal(res, &resp)
	if err != nil {
		fmt.Println("Invalid response!!")
		return -1
	}
	return resp.Result
}

func finalreport(successflag bool){

	fmt.Print("Result: ")
	if successflag{
		fmt.Println("success")
	}else{
		fmt.Println("fail")
	}

	fmt.Println("Insertion: ",successinsert,"/",totalinsert)

	if(len(inslis)==0||len(getlis)==0){
		fmt.Println("Due to 0 insertion or get operation, no latency report")
		return
	}

	var avg1,avg2 int64=0,0
	sort.Sort(inslis)
	sort.Sort(getlis)

	for i:=0; i<len(inslis); i++{
		//fmt.Println(inslis[i]/1000000)
		inslis[i]/=1000000
		avg1+=inslis[i]
	}

	for i:=0; i<len(getlis); i++{
	//	fmt.Println(getlis[i]/1000000)
		getlis[i]/=1000000
		avg2+=getlis[i]
	}

	avg1/=(int64)(len(inslis))
	avg2/=(int64)(len(getlis))

	fmt.Printf("Average latency(ms): %d/%d\n",avg1,avg2)

	var idx1,idx2,idx3,idx4 int=0,0,0,0
	var ix1,ix2,ix3,ix4 int=0,0,0,0

	idx1=len(inslis)/5
	idx2=len(inslis)/2
	idx3=len(inslis)*7/10
	idx4=len(inslis)*9/10

	ix1=len(getlis)/5
	ix2=len(getlis)/2
	ix3=len(getlis)*7/10
	ix4=len(getlis)*9/10

	fmt.Printf("Percentile latency(ms): %d/%d,%d/%d,%d/%d,%d/%d\n",inslis[idx1],getlis[ix1],inslis[idx2],getlis[ix2],inslis[idx3],getlis[ix3],inslis[idx4],getlis[ix4])
}

func cleanwork() (ret bool) {

	b1,data:=Dump(PrimaryIP)

	if(!b1){
		fmt.Println("clean failed")
		return false
	}

	for i:=0;i<NumPeers;i++ {

		BackupIP := (strings.Split(Peers[i],":"))[0]
		b2, dumpres := Dump(BackupIP)

		if(!b2){
			fmt.Println("clean failed")
			return false
		}

		for k, v := range dumpres {
			v2, ok2 := data[k];
			if !ok2 {
				fmt.Println("clean failed")
				fmt.Println("inconsistency!")
				return false
			}
			if v2!=v {
				fmt.Println("clean failed")
				fmt.Println("inconsistency!!")
				return false
			}
		}

		for k, v := range data {
			v2, ok2 := dumpres[k];
			if !ok2 {
				fmt.Println("clean failed")
				fmt.Println("inconsistency!!!")
				return false
			}
			if v2!=v {
				fmt.Println("clean failed")
				fmt.Println("inconsistency!!!!")
				return false
			}
		}
	}

	for k, v := range data {
		t,s:=Delete(k)
		if(!t){
			fmt.Println("clean failed")
			fmt.Println("delete failed!!!!"+" "+k)
			return false
		}
		if(s!=v){
			fmt.Println("clean failed")
			fmt.Println("inconsistency!!!!!")
			return false
		}
	}


	for i:=0;i<NumPeers;i++ {
		BackupIP := (strings.Split(Peers[i],":"))[0]
		b2, dumpres := Dump(BackupIP)

		if(!b2){
			fmt.Println("clean failed!")
			return false
		}

		if(Countkey(BackupIP)!=0||len(dumpres)!=0){
			fmt.Println("clean failed!")
			fmt.Println("countkey!=0orlen(data)!=0")
			return false
		}
	}

	fmt.Println("clean successful!")

	return true
}

func advancedtest(timeout,down,up int){

	if(down>up){
		tmp:=down
		down=up
		up=tmp
	}
	if(timeout<500) {
		timeout=500
	}

	var starttime time.Time=time.Now()
	var hash [10001]int
	var value [10001]string

	for i:=0;i<len(hash);i++ {
		hash[i]=0
		value[i]=""
	}

	var successflag bool=true
	for true{
		if time.Now().Sub(starttime).Nanoseconds()/1000000>(int64)(timeout){
			break
		}
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		idx:= r.Intn(up-down+1)+down

		if hash[idx]==0{
			k:="key "+fmt.Sprintf("%d",idx)
			v:="value "
			for j:=0; j<valuelen; j++ {
				v=v+fmt.Sprintf("%d", r.Intn(10))
			}
			var b bool
			b=Insert(k, v)
			if(b) {
				hash[idx]=1
				value[idx]=v
				fmt.Println("Insert ",idx," successful!")
			}else{
				fmt.Println("Insert ",idx," useless")
			}
		}else{
			op:=r.Intn(10)
			if op==0 {
				var check bool
				var ss string
				check,ss=Delete(fmt.Sprintf("key %d",idx))
				if !check{
					fmt.Println("Delete ",idx," failed!")
					successflag=false
					finalreport(successflag)
					return
				}
				if ss!=value[idx]{
					fmt.Println(idx," inconsistency!?")
					successflag=false
					finalreport(successflag)
					return
				}
				hash[idx]=0
				value[idx]=""
				fmt.Println("Delete ",idx," successful!")
			} else if op<5{
				var check bool
				var ss string
				check,ss=Get(fmt.Sprintf("key %d",idx))
				if !check{
					fmt.Println("Get ",idx," failed!")
					successflag=false
					finalreport(successflag)
					return
				}
				if ss!=value[idx]{
					fmt.Println(idx," inconsistency??")
					successflag=false
					finalreport(successflag)
					return
				}
				fmt.Println("Get ",idx," successful!")
			}else{
				var k string
				var v string
				k="key "+fmt.Sprintf("%d",idx)
				v="value "
				for j:=0; j<valuelen; j++ {
					v=v+fmt.Sprintf("%d", r.Intn(10))
				}
				var check bool
				check=Update(k,v)
				if !check{
					fmt.Println("Update",idx,"  failed!")
					successflag=false
					finalreport(successflag)
					return
				}
				value[idx]=v
				fmt.Println("Update",idx,"  successful!")
			}
		}
	}
	fmt.Println("Timeout!!!")
	fmt.Println("Free resources")
	for i:=0;i<len(hash);i++{
		if(hash[i]==1){
			idx:=i
			var check bool
			var ss string
			check,ss=Delete(fmt.Sprintf("key %d",idx))
			if !check{
				fmt.Println("Delete",idx,"  failed!")
				successflag=false
				finalreport(successflag)
				return
			}
			if ss!=value[idx]{
				fmt.Println(idx," inconsistency!?")
				successflag=false
				finalreport(successflag)
				return
			}
			hash[idx]=0
			value[idx]=""
			fmt.Println("Delete",idx,"  successful!")
		}
	}
	fmt.Println("Finish")
	finalreport(successflag)
}

func test(args []string){

	if(len(args)>=2&&args[1]=="clean"){
		var id int
		if(len(args)>2) {
			n,_:=fmt.Sscanf(args[2],"%d",&id)
			if n!=1 || id < 1 || id > NumPeers {
				fmt.Println("Invalid arguments")
				return
			}
			PrimaryIP = strings.Split(Peers[id-1],":")[0]
		}
		cleanwork();
	}else if(args[1]=="advanced"&& len(args)>=3) {
		var l,r int=0,0
		var timeout,id int
		if(len(args)>=3){
			n,_:=fmt.Sscanf(args[2],"%d",&timeout)
			if n!=1||timeout>600000 {
				fmt.Println("Invalid arguments")
				return
			}
		}

		if(len(args)>=4){
			n,_:=fmt.Sscanf(args[3],"%d",&l)
			if n!=1 ||l<0||l>10000{
				fmt.Println("Invalid arguments")
				return
			}
		}
		if(len(args)>=5){
			n,_:=fmt.Sscanf(args[4],"%d",&r)
			if n!=1 ||r<0||r>10000{
				fmt.Println("Invalid arguments")
				return
			}
		}
		if(len(args)>=6) {
			n,_:=fmt.Sscanf(args[5],"%d",&id)
			if n!=1 || id < 1 || id > NumPeers {
				fmt.Println("Invalid arguments")
				return
			}
			PrimaryIP = strings.Split(Peers[id-1],":")[0]
		}

		advancedtest(timeout,l,r)
	}

}


func main() {
	configpath := "conf/settings.conf"

	var config = map[string]string{}
	bytes, err := ioutil.ReadFile(configpath)
	if err!=nil {
		fmt.Println("error on config")
		return
	}
	err = json.Unmarshal(bytes, &config)
	if err!=nil {
		fmt.Println("error on config")
		return
	}

	var Exists bool
	ServerPort, Exists = config["port"]
	if !Exists {
		fmt.Println("error in config: no port")
		return
	} else {
		n, _ := fmt.Sscanf(ServerPort,"%d", &ServerIntPort)
		if n!=1 {
			fmt.Println("error in config: port invalid")
			return
		}
	}

	for i:=1; true; i++ {
		var nodename string

		if i < 10 {
			nodename=fmt.Sprintf("n0%d",i)
		} else {
			nodename=fmt.Sprintf("n%d",i)
		}

		var nodeipport string
		nodeipport, Exists = config[nodename]
		if !Exists {
			break
		}

		Peers = append(Peers, nodeipport)
		//fmt.Println(Peers[i-1])
	}

	NumPeers = len(Peers)

	PrimaryIP = strings.Split(Peers[0],":")[0]
	Port = fmt.Sprintf("%d",ServerIntPort)

	args := os.Args
	if(args == nil || len(args) < 2){
		return
	} else{
		test(args)
	}
}
