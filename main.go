package main

import (
	"encoding/json"
	"fmt"
	slog "github.com/sirupsen/logrus"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

type kvFsm struct {
	db *sync.Map
}

type setPayload struct {
	Key   string
	Value string
}

type CommandPayLoad struct {
	Key    string
	Value  string
	Type   string
	Method string
}

// 定义了一些key的操作类型
const (
	LogTypeSet = "set"
	LogTypeGet = "get"
	LogTypeDel = "delete"
)

func (kf *kvFsm) Apply(_log *raft.Log) any {

	switch _log.Type {
	case raft.LogCommand:
		var sp CommandPayLoad
		err := json.Unmarshal(_log.Data, &sp)
		if err != nil {
			return fmt.Errorf("Could not parse payload: %s", err)
		}
		switch sp.Type {
		case LogTypeSet:
			kf.db.Store(sp.Key, sp.Value)
		case LogTypeDel:
			kf.db.Delete(sp.Key)
		default:
			slog.Errorf("unkown log type:%s", sp.Type)
		}

		//time.Sleep(time.Second * time.Duration(rand.N(5)))
	default:
		return fmt.Errorf("Unknown raft _log type: %#v", _log.Type)
	}
	slog.Infof("finish process raft _log:%d,%s", _log.Type, _log.Data)
	return nil
}

type snapshotNoop struct {
	data map[string]interface{} // 存储快照数据
}

// Persist 用来生成快照数据，一般只需要实现它即可
func (s snapshotNoop) Persist(sink raft.SnapshotSink) error {
	// 将snapshot的数据转成json
	snapshotBytes, err := json.Marshal(s.data)
	if err != nil {
		sink.Cancel()
		return err
	}

	// 进行数据持久化存储
	if _, err := sink.Write(snapshotBytes); err != nil {
		sink.Cancel()
		return err
	}
	if err := sink.Close(); err != nil {
		sink.Cancel()
		return err
	}
	return nil

}

// Release 生成快照后的回调通知
func (s snapshotNoop) Release() {
	keyCount := len(s.data)
	slog.Infof("snapshot has finished, key count:%d", keyCount)
}

// Snapshot FSM需要提供的另外两个方法是Snapshot()和Restore()
// Snapshot()方法用于生成快照，Restore()方法用于根据快照恢复数据
func (kf *kvFsm) Snapshot() (raft.FSMSnapshot, error) {
	// 生成快照数据
	SnapData := make(map[string]interface{})
	kf.db.Range(func(key, value interface{}) bool {
		SnapData[key.(string)] = value
		return true
	})
	return snapshotNoop{SnapData}, nil
}

// Restore 用于恢复数据
func (kf *kvFsm) Restore(rc io.ReadCloser) error {
	decoder := json.NewDecoder(rc)

	for decoder.More() {
		var sp setPayload
		err := decoder.Decode(&sp)
		if err != nil {
			return fmt.Errorf("could not decode payload: %s", err)
		}
		kf.db.Store(sp.Key, sp.Value)
	}

	return rc.Close()
}

// 初始化raft相关的配置
func setupRaft(dir, nodeId, raftAddress string, kf *kvFsm) (*raft.Raft, error) {
	slog.Info("start to setup raft")
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("could not create data directory: %s", err)
	}

	store, err := raftboltdb.NewBoltStore(path.Join(dir, "bolt"))
	if err != nil {
		return nil, fmt.Errorf("could not create bolt store: %s", err)
	}

	// 快照存储，使用文件进行存储，存储路径data/snapshot
	snapshots, err := raft.NewFileSnapshotStore(path.Join(dir, "snapshot"), 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create snapshot store: %s", err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, fmt.Errorf("could not resolve address: %s", err)
	}

	transport, err := raft.NewTCPTransport(raftAddress, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create tcp transport: %s", err)
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(nodeId)
	raftCfg.SnapshotThreshold = 20000
	raftCfg.SnapshotInterval = 3000 * time.Second

	r, err := raft.NewRaft(raftCfg, kf, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("could not create raft instance: %s", err)
	}

	// Cluster consists of unjoined leaders. Picking a leader and
	// creating a real cluster is done manually after startup.
	r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(nodeId),
				Address: transport.LocalAddr(),
			},
		},
	})

	return r, nil
}

type httpServer struct {
	r  *raft.Raft
	db *sync.Map
}

type NodeAddr struct {
	Addr string `json:"addr"`
	ID   string `json:"id"`
}

type NodeInfo struct {
	Leader NodeAddr   `json:"leader"`
	Index  uint64     `json:"index"`
	Nodes  []NodeAddr `json:"nodes"`
}

func (hs httpServer) GetNodesInfo(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	addr, leaderID := hs.r.LeaderWithID()
	config := hs.r.GetConfiguration()
	nodes := make([]NodeAddr, 0)
	for _, srv := range config.Configuration().Servers {
		nodes = append(nodes, NodeAddr{string(srv.Address), string(srv.ID)})
	}
	leader := NodeAddr{string(addr), string(leaderID)}

	// 获取最后一个被应用的日志条目的索引
	rsp := NodeInfo{leader, hs.r.LastIndex(), nodes}

	slog.Infof("leader addr:%s, leader id:%s", addr, leaderID)
	slog.Info("stats:", hs.r.GetConfiguration())
	slog.Infof("state:%s, %v", hs.r.State(), hs.r.Stats())
	err := json.NewEncoder(w).Encode(rsp)
	if err != nil {
		slog.Infof("Could not encode key-value in http response: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
	w.WriteHeader(http.StatusOK)
}

func (hs httpServer) writeKey(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// 读取请求体
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Infof("无法读取HTTP请求中的键值对: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	// 解析请求体为setPayload结构体
	var sp setPayload
	if err = json.Unmarshal(bs, &sp); err != nil {
		slog.Infof("请求体错误: %s", bs)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	// 构造logCommand结构体
	logCommand := CommandPayLoad{
		Key:    sp.Key,
		Value:  sp.Value,
		Method: r.Method,
		Type:   strings.ToLower(strings.TrimLeft(r.URL.Path, "/")),
	}

	// 将logCommand结构体序列化为JSON
	data, err := json.Marshal(logCommand)
	if err != nil {
		slog.Infof("序列化logCommand失败: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// 应用数据并设置超时
	future := hs.r.Apply(data, 500*time.Millisecond)
	if err = future.Error(); err != nil {
		slog.Infof("无法写入键值对: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	// 获取响应
	e := future.Response()
	if e != nil {
		slog.Infof("无法写入键值对，应用错误: %s", e)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	// 设置响应状态码
	w.WriteHeader(http.StatusOK)
}

func (hs httpServer) getKey(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value, _ := hs.db.Load(key)
	if value == nil {
		value = ""
	}

	rsp := struct {
		Data string `json:"data"`
	}{value.(string)}
	err := json.NewEncoder(w).Encode(rsp)
	if err != nil {
		slog.Infof("Could not encode key-value in http response: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}

func (hs httpServer) joinHandler(w http.ResponseWriter, r *http.Request) {
	followerId := r.URL.Query().Get("followerId")
	followerAddr := r.URL.Query().Get("followerAddr")

	if hs.r.State() != raft.Leader {
		slog.Info("not need")
		json.NewEncoder(w).Encode(struct {
			Error string `json:"error"`
		}{
			"Not the leader",
		})

		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	err := hs.r.AddVoter(raft.ServerID(followerId), raft.ServerAddress(followerAddr), 0, 0).Error()
	if err != nil {
		slog.Infof("Failed to add follower: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}

	w.WriteHeader(http.StatusOK)
}

type config struct {
	id       string
	httpPort string
	raftPort string
}

func getConfig() config {
	cfg := config{}
	for i, arg := range os.Args[1:] {
		if arg == "--node-id" {
			cfg.id = os.Args[i+2]
			i++
			continue
		}

		if arg == "--http-port" {
			cfg.httpPort = os.Args[i+2]
			i++
			continue
		}

		if arg == "--raft-port" {
			cfg.raftPort = os.Args[i+2]
			i++
			continue
		}
	}

	if cfg.id == "" {
		log.Fatal("Missing required parameter: --node-id")
	}

	if cfg.raftPort == "" {
		log.Fatal("Missing required parameter: --raft-port")
	}

	if cfg.httpPort == "" {
		log.Fatal("Missing required parameter: --http-port")
	}

	return cfg
}

func initLog() {
	//slog.SetReportCaller(true)
}

func main() {
	initLog()
	cfg := getConfig()

	db := &sync.Map{}
	kf := &kvFsm{db}

	// 初始化raft配置
	dataDir := "data"
	r, err := setupRaft(path.Join(dataDir, "raft"+cfg.id), cfg.id, "localhost:"+cfg.raftPort, kf)
	if err != nil {
		log.Fatal(err)
	}

	hs := httpServer{r, db}

	// key-value操作
	http.HandleFunc("/set", hs.writeKey) // 设置key
	http.HandleFunc("/delete", hs.writeKey)
	http.HandleFunc("/get", hs.getKey)

	// raft操作
	http.HandleFunc("/join", hs.joinHandler)   // 加入集群
	http.HandleFunc("/state", hs.GetNodesInfo) // 查询节点的状态信息，包括机器列表，leader地址

	// 启动web服务
	slog.Info("http server start")
	http.ListenAndServe(":"+cfg.httpPort, nil)
}
