/**
 * @Author: DPY
 * @Description:
 * @File:  client
 * @Version: 1.0.0
 * @Date: 2021/11/22 11:07
 */

package dgraph

import (
	"context"
	"errors"
	"github.com/dgraph-io/dgo/v210"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
)

// Config dgraph数据库配置
// Targets = 请求目标,格式：[ '172.21.192.25:9080','172.21.192.26:9080','172.21.192.27:9080' ]
// Username = (可选)认证用户名
// Password = (可选)认证密码
type Config struct {
	Targets  []string `yaml:"targets,omitempty" form:"targets,omitempty" json:"targets,omitempty"`
	Username string   `yaml:"username,omitempty" form:"username,omitempty" json:"username,omitempty"`
	Password string   `yaml:"password,omitempty" form:"password,omitempty" json:"password,omitempty"`
}

func (c Config) NewClient() (*Client, error) {
	var (
		err             error
		wg              sync.WaitGroup
		clients         []api.DgraphClient
		ctx, cancelFunc = context.WithCancel(context.Background())
		opts            = []grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(256<<20), grpc.MaxCallRecvMsgSize(256<<20)),
		}
		conn []*grpc.ClientConn
	)
	defer func() {
		if err != nil {
			cancelFunc()
		}
	}()
	if len(c.Targets) == 0 {
		err = errors.New("no dgraph targets in config")
		return nil, err
	}
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	for _, target := range c.Targets {
		wg.Add(1)
		go func(tgt string) {
			defer wg.Done()
			grpcConn, e := grpc.DialContext(ctx, tgt, opts...)
			if e != nil {
				err = e
				return
			}
			cl := api.NewDgraphClient(grpcConn)
			clients = append(clients, cl)
			conn = append(conn, grpcConn)
		}(target)
	}
	wg.Wait()
	if err != nil {
		return nil, err
	}
	if len(clients) == 0 {
		err = errors.New("no dgraph targets connected")
		return nil, err
	}
	dgraph := dgo.NewDgraphClient(clients...)
	return &Client{Dgraph: dgraph, cancel: cancelFunc}, nil
}

type Client struct {
	*dgo.Dgraph
	cancel context.CancelFunc
	closed bool
}

func (d *Client) Txn(ronly ...bool) *Txn {
	d.Dgraph.NewTxn()
	if len(ronly) > 0 && ronly[0] {
		txn := d.NewReadOnlyTxn()
		txn = txn.BestEffort()
		return &Txn{Txn: txn}
	}
	return &Txn{Txn: d.NewTxn()}
}

func (d *Client) SetSchemaPred(pred SchemaPred) error {
	err := d.Alter(context.Background(), &api.Operation{
		Schema: pred.Rdf(),
	})
	return err
}

func (d *Client) SetPred(pred Pred) error {
	err := d.Alter(context.Background(), &api.Operation{
		Schema: pred.Rdf(),
	})
	return err
}

// DropPred 删除谓词
func (d *Client) DropPred(name string) error {
	err := d.Alter(context.Background(), &api.Operation{
		DropValue: name,
		DropOp:    api.Operation_ATTR,
	})
	return err
}

// SetSchemaType 设置schema类型
func (d *Client) SetSchemaType(t SchemaType) error {
	err := d.Alter(context.Background(), &api.Operation{
		Schema: t.Rdf(),
	})
	return err
}

// SetType 设置类型
func (d *Client) SetType(t Type) error {
	err := d.Alter(context.Background(), &api.Operation{
		Schema: t.Schema().Rdf(),
	})
	return err
}

// DropType 删除类型
func (d *Client) DropType(name string) error {
	err := d.Alter(context.Background(), &api.Operation{
		DropValue:       name,
		DropOp:          api.Operation_TYPE,
		RunInBackground: false,
	})
	return err
}

// DropAllData 删除所有数据
func (d *Client) DropAllData() error {
	err := d.Alter(context.Background(), &api.Operation{
		DropOp: api.Operation_DATA,
	})
	return err
}

// DropAllDataAndSchema 删除所有数据和结构
func (d *Client) DropAllDataAndSchema() error {
	err := d.Alter(context.Background(), &api.Operation{
		DropAll: true,
	})
	return err
}

var ErrMutt = errors.New("没有数据被处理，可能不满足数据的插入约束条件")

// CheckResponse 检查变更的返回值
// 返回变更产生的UID列表，变更是否成功，以及是否存在错误
func CheckResponse(resp *api.Response) ([]string, error) {
	var r []string
	if len(resp.Uids) > 0 {
		for _, v := range resp.Uids {
			r = append(r, v)
		}
		return r, nil
	}
	//return nil, nil
	if resp.Txn != nil && len(resp.Txn.Preds) > 0 {
		return nil, nil
	}
	return nil, ErrMutt
}
