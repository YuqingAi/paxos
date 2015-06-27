package main

import (
	"fmt"
	"net/http"
	"net/url"
	"io/ioutil"
	"bytes"
	"encoding/json"
	"time"
)

var IP []string
var Port string
var Timeout time.Duration


type StatusFmt struct {
	Success	bool	`json:"success"`
}
type ValueFmt struct {
	Success	bool	`json:"success"`
	Value	string	`json:value`
}

func Insert(key string, value string, addr string) StatusFmt {
	rvalue := StatusFmt{}
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Server not responding!")
		}
	}()
	data := url.Values{"key":{key}, "value":{value}}
	req, _ := http.Post("http://"+addr+"/kv/insert", "application/x-www-form-urlencoded", bytes.NewBufferString(data.Encode()))
	fmt.Println(req)
	defer req.Body.Close()
	res, _ := ioutil.ReadAll(req.Body)
	fmt.Println(res)
	json.Unmarshal(res, &rvalue)
	return rvalue
}

func Get(key string, addr string) ValueFmt {
	rvalue := ValueFmt{}
	tmp:=url.Values{}
	tmp.Set("key", key)
	fmt.Println(addr)
	fmt.Println(tmp.Encode())
	req, _ := http.Get("http://"+addr+"/kv/get?"+tmp.Encode())
	defer req.Body.Close()
	res, _ := ioutil.ReadAll(req.Body)
	json.Unmarshal(res, &rvalue)
	return rvalue
}

func Delete(key string, addr string) ValueFmt {
	rvalue := ValueFmt{}
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Server not responding!")
		}
	}()
	data := url.Values{"key":{key}}
	req, _ := http.Post("http://"+addr+"/kv/delete", "application/x-www-form-urlencoded", bytes.NewBufferString(data.Encode()))
	defer req.Body.Close()
	res, _ := ioutil.ReadAll(req.Body)
	json.Unmarshal(res, &rvalue)
	return rvalue
}

func Update(key string, value string, addr string) StatusFmt {
	rvalue := StatusFmt{}
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Server not responding!")
		}
	}()
	data := url.Values{"key":{key}, "value":{value}}
	req, _ := http.Post("http://"+addr+"/kv/update", "application/x-www-form-urlencoded", bytes.NewBufferString(data.Encode()))
	defer req.Body.Close()
	res, _ := ioutil.ReadAll(req.Body)
	json.Unmarshal(res, &rvalue)
	return rvalue
}

func Countkey(addr string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Server not responding!")
		}
	}()
	req, _ := http.Post("http://"+addr+"/kvman/countKey", "application/x-www-form-urlencoded", nil)
	defer req.Body.Close()
	res, _ := ioutil.ReadAll(req.Body)
	fmt.Println(string(res))
}

func Dump(addr string) string {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Server not responding!")
		}
	}()
	req, _ := http.Post("http://"+addr+"/kvman/dump", "application/x-www-form-urlencoded", nil)
	defer req.Body.Close()
	res, _ := ioutil.ReadAll(req.Body)
	return string(res)
}

func Shutdown(addr string) {
	defer func() {
		if r := recover(); r != nil {
            fmt.Println("Server not responding!")
		}
	}()
	http.Post("http://"+addr+"/kvman/shutdown", "application/x-www-form-urlencoded", nil)
}

func Init() {
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
	IP[0], exist1 = config["n01"]
	IP[1], exist2 = config["n02"]
	IP[2], exist3 = config["n03"]
	Port, exist4 = config["port"]
	if (!exist1 || !exist2 || !exist3 || !exist4) {
		fmt.Println("Configuration file: Bad format!")
		return
	}

	//	for i:=0; i<3; i++ {
	//		IP[i] += Port
	//	}
	IP[0] += ":12345"
	IP[1] += ":54321"
	IP[2] += ":11111"

}


func main() {
	var config = map[string]string{}
	Timeout = 5.0 * time.Second
	bytes, err := ioutil.ReadFile("src/conf/settings.conf")

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



	fmt.Println("test start...")
	fmt.Println(Insert("key1", "value1", IP[0]))
	fmt.Println(Get("key1", IP[0]))
	fmt.Println(Get("key1", IP[1]))
	fmt.Println(Get("key1", IP[2]))
	fmt.Println(Delete("key1", IP[0]))
	fmt.Println(Get("key1", IP[0]))
	fmt.Println(Get("key1", IP[1]))
	fmt.Println(Get("key1", IP[2]))
	fmt.Println(Insert("key1", "value2", IP[0]))
	fmt.Println(Get("key1", IP[0]))
	fmt.Println(Get("key1", IP[1]))
	fmt.Println(Get("key1", IP[2]))
	fmt.Println(Update("key1", "value3", IP[0]))
	fmt.Println(Get("key1", IP[0]))
	fmt.Println(Get("key1", IP[1]))
	fmt.Println(Get("key1", IP[2]))
}

