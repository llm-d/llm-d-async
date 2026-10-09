package redis

import (
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
)

func TestUniversalOptions_MapsURLAndTopology(t *testing.T) {
	t.Run("standalone keeps a single address and the database", func(t *testing.T) {
		o, err := universalOptions(ConnectionConfig{URL: "redis://user:pw@h1:6379/2?pool_size=7"})
		if err != nil {
			t.Fatal(err)
		}
		if len(o.Addrs) != 1 || o.Addrs[0] != "h1:6379" || o.DB != 2 || o.Username != "user" || o.Password != "pw" || o.PoolSize != 7 {
			t.Fatalf("unexpected options: %+v", o)
		}
		if o.IsClusterMode || o.MasterName != "" {
			t.Fatalf("standalone must not request cluster or failover: %+v", o)
		}
	})
	t.Run("cluster adds seeds and sets cluster mode even with one address", func(t *testing.T) {
		o, err := universalOptions(ConnectionConfig{URL: "rediss://h1:7000", Mode: ModeCluster, Addrs: []string{"h2:7000", "h3:7000"}})
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(o.Addrs, ",") != "h1:7000,h2:7000,h3:7000" || !o.IsClusterMode || o.TLSConfig == nil {
			t.Fatalf("unexpected options: %+v", o)
		}
		single, err := universalOptions(ConnectionConfig{URL: "redis://cfg-endpoint:6379", Mode: ModeCluster})
		if err != nil {
			t.Fatal(err)
		}
		if !single.IsClusterMode {
			t.Fatal("a single configuration endpoint must still produce a cluster client")
		}
	})
	t.Run("sentinel carries the master name and sentinel addresses", func(t *testing.T) {
		o, err := universalOptions(ConnectionConfig{URL: "redis://:pw@s1:26379/1", Mode: ModeSentinel, Addrs: []string{"s2:26379"}, MasterName: "mymaster"})
		if err != nil {
			t.Fatal(err)
		}
		if o.MasterName != "mymaster" || strings.Join(o.Addrs, ",") != "s1:26379,s2:26379" || o.DB != 1 || o.IsClusterMode {
			t.Fatalf("unexpected options: %+v", o)
		}
	})
}

func TestUniversalOptions_RejectsInconsistentTopology(t *testing.T) {
	cases := map[string]struct {
		cfg  ConnectionConfig
		want string
	}{
		"missing url":             {ConnectionConfig{}, "required"},
		"bad url":                 {ConnectionConfig{URL: "nope://"}, "parse"},
		"unknown mode":            {ConnectionConfig{URL: "redis://h:1", Mode: "replicated"}, "unknown mode"},
		"addrs without mode":      {ConnectionConfig{URL: "redis://h:1", Addrs: []string{"h2:1"}}, "addrs requires mode"},
		"master without sentinel": {ConnectionConfig{URL: "redis://h:1", MasterName: "m"}, "master_name requires mode"},
		"master with cluster":     {ConnectionConfig{URL: "redis://h:1", Mode: ModeCluster, MasterName: "m"}, "master_name requires mode"},
		"sentinel without master": {ConnectionConfig{URL: "redis://h:1", Mode: ModeSentinel}, "master_name is required"},
		"cluster with database":   {ConnectionConfig{URL: "redis://h:1/3", Mode: ModeCluster}, "database 3"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := universalOptions(tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestNewUniversalClient_SelectsClientPerMode(t *testing.T) {
	cases := map[string]struct {
		cfg     ConnectionConfig
		cluster bool
	}{
		"standalone": {ConnectionConfig{URL: "redis://h:6379"}, false},
		"explicit":   {ConnectionConfig{URL: "redis://h:6379", Mode: ModeStandalone}, false},
		"cluster":    {ConnectionConfig{URL: "redis://h:7000", Mode: ModeCluster}, true},
		"sentinel":   {ConnectionConfig{URL: "redis://h:26379", Mode: ModeSentinel, MasterName: "m"}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c, err := newUniversalClient(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			_, isCluster := c.(*redis.ClusterClient)
			if isCluster != tc.cluster {
				t.Fatalf("client type %T, want cluster=%v", c, tc.cluster)
			}
		})
	}
}

func TestTransportConfigs_ParseAndValidateTopology(t *testing.T) {
	const cluster = `{"url":"redis://h1:7000","mode":"cluster","addrs":["h2:7000"],"queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}`
	const sentinel = `{"url":"redis://s1:26379","mode":"sentinel","master_name":"mymaster","queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}`
	const badAddrs = `{"url":"redis://h1:6379","addrs":["h2:6379"],"queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}`
	const badMode = `{"url":"redis://h1:6379","mode":"ring","queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}`

	ss, err := LoadSortedSetConfig([]byte(cluster))
	if err != nil {
		t.Fatalf("sortedset cluster config: %v", err)
	}
	if ss.Mode != ModeCluster || len(ss.Addrs) != 1 || ss.connection().URL != "redis://h1:7000" {
		t.Fatalf("sortedset cluster fields not parsed: %+v", ss)
	}
	ps, err := LoadPubSubConfig([]byte(sentinel))
	if err != nil {
		t.Fatalf("pubsub sentinel config: %v", err)
	}
	if ps.Mode != ModeSentinel || ps.MasterName != "mymaster" {
		t.Fatalf("pubsub sentinel fields not parsed: %+v", ps)
	}
	for name, raw := range map[string]string{"addrs without mode": badAddrs, "unknown mode": badMode} {
		if _, err := LoadSortedSetConfig([]byte(raw)); err == nil {
			t.Errorf("sortedset %s: expected validation error", name)
		}
		if _, err := LoadPubSubConfig([]byte(raw)); err == nil {
			t.Errorf("pubsub %s: expected validation error", name)
		}
	}
}

func TestNewRedisSortedSetFlow_ClusterModeSelectsClientAndLayout(t *testing.T) {
	// miniredis answers CLUSTER SLOTS as a single-node cluster, so a genuine
	// cluster client bootstraps against it.
	s := miniredis.RunT(t)
	cfg := SortedSetConfig{
		URL:  "redis://" + s.Addr(),
		Mode: ModeCluster,
		Queues: []SortedSetQueueConfig{{
			QueueName: "q", IGWBaseURL: "http://gw", WorkerPoolID: "default",
		}},
	}
	cfg.ApplyDefaults()
	flow, err := NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer flow.Shutdown()
	if _, ok := flow.rdb.(*redis.ClusterClient); !ok {
		t.Fatalf("flow client is %T, want a cluster client", flow.rdb)
	}
	if !flow.clusterKeys {
		t.Fatal("cluster mode must select the hash-tagged key layout")
	}
	if err := flow.rdb.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("ping through the cluster client: %v", err)
	}

	standalone := cfg
	standalone.Mode = ""
	plain, err := NewRedisSortedSetFlow(standalone, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Shutdown()
	if _, ok := plain.rdb.(*redis.Client); !ok || plain.clusterKeys {
		t.Fatalf("standalone flow: client %T clusterKeys=%v", plain.rdb, plain.clusterKeys)
	}
}
