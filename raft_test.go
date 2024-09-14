package main

import (
	"encoding/json"
	"errors"
	"fmt"
	slog "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func SetUp() *http.Client {
	transport := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}

	// 创建一个自定义的 Client
	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}
	return client
}

type Cluster struct {
	Node    NodeInfo
	servers []string
	mutex   sync.Mutex
	i       int
}

func (c *Cluster) GetRandomServer() string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.i++
	return c.servers[c.i%len(c.servers)]
}

func (c *Cluster) GetLeader() string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return GetApiAddr(c.Node.Leader.Addr)
}

func (c *Cluster) RemoveServer(addr string) error {
	if len(c.servers) < 1 {
		return errors.New("no available addr")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i, _addr := range c.servers {
		if addr == _addr {
			c.servers = append(c.servers[:i], c.servers[i+1:]...)
			break
		}
	}
	return nil
}

func (c *Cluster) SetServers(servers []string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, _addr := range servers {
		c.servers = append(c.servers, _addr)
	}
}

func (c *Cluster) InitNodes(client *http.Client, addr string) error {
	// 初始化步骤：根据addr获取节点信息
	// 如果addr为空，获取leader
	// 如果leader访问失败或者为空，重新刷新
	slog.Infof("init nodes: %s", addr)
	slog.Infof("now servers: %v", c.servers)
	if len(c.servers) > 0 && addr == "" {
		addr = c.GetRandomServer()
	} else if addr == "" {
		slog.Errorf("no available addr")
		return errors.New("no available addr")
	}
	//刷新
	var resp *http.Response
	var err error

	i := 0
	for i < 50 {
		i++
		resp, err = client.Get(addr + "/state")

		if err != nil || resp.StatusCode != 200 {
			slog.Errorf("error addr: %s, retry %d time", addr, i)
			slog.Errorf("error: %s", err)
			if resp != nil {
				body, _ := io.ReadAll(resp.Body)
				slog.Errorf("resp: %s", string(body))
			}
			time.Sleep(2 * time.Second)
			// change addr
			if i > 10 {
				c.RemoveServer(addr)
				if len(c.servers) > 0 {
					addr = c.GetRandomServer()
				}
				i = 0
			}
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		slog.Infof("resp: %s", string(body))
		c.mutex.Lock()
		json.Unmarshal(body, &c.Node)
		c.mutex.Unlock()
		if c.Node.Leader.Addr == "" {
			time.Sleep(1 * time.Second)
			slog.Errorf("no leader")
			continue
			//return errors.New("no leader")
		}
		c.mutex.Lock()
		c.servers = []string{}
		for _, node := range c.Node.Nodes {
			c.servers = append(c.servers, GetApiAddr(node.Addr))
		}
		c.mutex.Unlock()
		return nil
	}
	return errors.New("init failed")
}

func GetApiAddr(raftAddr string) string {
	var IPPort = strings.Split(raftAddr, ":")
	port, _ := strconv.Atoi(IPPort[1])
	return fmt.Sprintf("http://%s:%d", IPPort[0], port+6000)
}

func GetLeader(client *http.Client, addr string) (string, error) {
	resp, err := client.Get(addr)

	if err != nil {
		slog.Errorf("err: %s", err)
		return "", fmt.Errorf("Request failed: %v\n", err)
	}
	body, _ := io.ReadAll(resp.Body)
	//var result struct {
	//	Leader struct {
	//		Addr string `json:"addr"`
	//		ID   string `json:"id"`
	//	} `json:"leader"`
	//}
	var result NodeInfo
	slog.Infof("resp: %s", string(body))
	json.Unmarshal(body, &result)
	// 获取的是raft的地址，web地址+6000
	return "", nil
	//return fmt.Sprintf("http://%s:%d", IPPort[0], port+6000), nil
}

func BenchmarkKeySet(b *testing.B) {
	b.StopTimer()
	// 启动服务
	// 创建集群
	initLog()
	client := SetUp()
	var c Cluster
	var err error
	c.SetServers([]string{"http://localhost:8222", "http://localhost:8223", "http://localhost:8224"})
	err = c.InitNodes(client, "")
	if err != nil {
		slog.Error(err)
		return
	}
	b.StartTimer()
	// 设置key
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("val_%d", i)
		body := fmt.Sprintf(`{"key":"%s","value":"%s"}`, key, value)
		resp, error := client.Post(c.GetLeader()+"/set", "application/json", strings.NewReader(body))
		//assert.Nil(b, error)
		if error != nil {
			fmt.Println(error)
			c.InitNodes(client, "")
			continue
		}
		assert.Equal(b, 200, resp.StatusCode)
		resp.Body.Close()
	}
}

func BenchmarkKeyGet(b *testing.B) {
	b.StopTimer()
	// 启动服务
	// 创建集群
	initLog()
	client := SetUp()

	var c Cluster
	var err error
	c.SetServers([]string{"http://localhost:8222", "http://localhost:8223", "http://localhost:8224"})
	err = c.InitNodes(client, "")
	if err != nil {
		slog.Error(err)
		return
	}

	b.StartTimer()

	// 读取key
	for i := 0; i < b.N; i++ {
		if i%100 == 0 {
			slog.Infof("get %d", i)
		}
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("val_%d", i)
		resp, error := client.Get(fmt.Sprintf("%s/get?key=%s", c.GetRandomServer(), key))
		if error != nil {
			slog.Error(error)
			c.InitNodes(client, "")
			continue
		}
		assert.Nil(b, error)
		assert.Equal(b, 200, resp.StatusCode)
		var D struct {
			Data string `json:"data"`
		}
		bytes, _ := io.ReadAll(resp.Body)
		json.Unmarshal(bytes, &D)
		assert.Equal(b, value, D.Data)
		resp.Body.Close()
	}
}

func BenchmarkKeyDelete(b *testing.B) {
	b.StopTimer()
	// 启动服务
	// 创建集群
	initLog()
	client := SetUp()
	var c Cluster
	var err error
	c.SetServers([]string{"http://localhost:8222", "http://localhost:8223", "http://localhost:8224"})
	err = c.InitNodes(client, "")
	if err != nil {
		slog.Error(err)
		return
	}

	b.StartTimer()

	// 删除key
	for i := 0; i < b.N; i++ {
		if i%1000 == 0 {
			slog.Infof("delete %d", i)
		}
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("val_%d", i)
		body := fmt.Sprintf(`{"key":"%s","value":"%s"}`, key, value)
		resp, error := client.Post(c.GetLeader()+"/delete", "application/json", strings.NewReader(body))
		if error != nil {
			fmt.Println(error)
			c.InitNodes(client, "")
			continue
		}
		assert.Nil(b, error)
		assert.Equal(b, 200, resp.StatusCode)
		resp.Body.Close()
	}
}
