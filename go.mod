module github.com/pingcap-incubator/tinykv

require (
	cloud.google.com/go/bigquery v1.4.0 // indirect
	cloud.google.com/go/firestore v1.1.1 // indirect
	github.com/BurntSushi/toml v0.3.1
	github.com/Connor1996/badger v1.5.1-0.20210202034640-5ff470f827f8
	github.com/antihax/optional v1.0.0 // indirect
	github.com/armon/go-metrics v0.3.10 // indirect
	github.com/census-instrumentation/opencensus-proto v0.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cilium/ebpf v0.8.0 // indirect
	github.com/cncf/udpa/go v0.0.0-20210930031921-04548b0d99d4 // indirect
	github.com/cncf/xds/go v0.0.0-20211130200136-a8f946100490 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/docker/go-units v0.4.0
	github.com/envoyproxy/go-control-plane v0.9.5 // indirect
	github.com/envoyproxy/protoc-gen-validate v0.6.2 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/fsnotify/fsnotify v1.5.1 // indirect
	github.com/go-delve/delve v1.8.0 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/btree v1.0.0
	github.com/google/martian/v3 v3.1.0 // indirect
	github.com/google/pprof v0.0.0-20210720184732-4bb14d4b1be1 // indirect
	github.com/google/uuid v1.1.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.14.5 // indirect
	github.com/hashicorp/consul/api v1.11.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/serf v0.9.6 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/juju/errors v0.0.0-20181118221551-089d3ea4e4d5
	github.com/juju/testing v0.0.0-20200510222523-6c8c298c77a0 // indirect
	github.com/magiconair/properties v1.8.5 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/mitchellh/mapstructure v1.4.3 // indirect
	github.com/onsi/ginkgo v1.12.1 // indirect
	github.com/onsi/gomega v1.10.0 // indirect
	github.com/opentracing/opentracing-go v1.0.2
	github.com/pelletier/go-toml v1.9.4 // indirect
	github.com/petar/GoLLRB v0.0.0-20190514000832-33fb24c13b99
	github.com/peterh/liner v1.2.2 // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errcode v0.0.0-20180921232412-a1a7271709d9
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/log v0.0.0-20200117041106-d28c14d3b1cd
	github.com/pingcap/tidb v1.1.0-beta.0.20200309111804-d8264d47f760
	github.com/pingcap/tipb v0.0.0-20200212061130-c4d518eb1d60
	github.com/pkg/errors v0.8.1
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/sagikazarmark/crypt v0.0.2 // indirect
	github.com/shirou/gopsutil v2.19.10+incompatible
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cast v1.4.1 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/viper v1.7.1 // indirect
	github.com/stretchr/testify v1.7.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.etcd.io/etcd/client/pkg/v3 v3.5.1 // indirect
	go.opencensus.io v0.22.5 // indirect
	go.uber.org/zap v1.17.0
	golang.org/x/arch v0.0.0-20210923205945-b76863e36670 // indirect
	golang.org/x/crypto v0.0.0-20210817164053-32db794688a5 // indirect
	golang.org/x/exp v0.0.0-20200224162631-6cc2880d07d6 // indirect
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd
	golang.org/x/sys v0.0.0-20220128215802-99c3d69c2c27 // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	google.golang.org/api v0.15.1 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/grpc v1.26.0
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.1.0 // indirect
	gopkg.in/ini.v1 v1.66.2 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	honnef.co/go/tools v0.0.1-2020.1.4 // indirect
	rsc.io/quote/v3 v3.1.0 // indirect
)

go 1.13

replace github.com/pingcap/tidb => github.com/pingcap-incubator/tinysql v0.0.0-20200518090433-a7d00f9e6aa7
