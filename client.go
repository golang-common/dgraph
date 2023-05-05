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

type Option func(client *config)

func WithUserAuth(username, password string) Option {
	return func(config *config) {
		config.username = username
		config.password = password
	}
}

type config struct {
	username string
	password string
	err      error
}

func NewClient(targets []string, options ...Option) (*Client, error) {
	var (
		cfg             config
		err             error
		wg              sync.WaitGroup
		clients         []api.DgraphClient
		ctx, cancelFunc = context.WithCancel(context.Background())
		opts            = []grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(256<<20), grpc.MaxCallRecvMsgSize(256<<20)),
		}
	)
	defer func() {
		if err != nil {
			cancelFunc()
		}
	}()
	for _, opt := range options {
		opt(&cfg)
		if cfg.err != nil {
			err = cfg.err
			return nil, err
		}
	}
	if len(targets) == 0 {
		err = errors.New("no dgraph targets in config")
		return nil, err
	}
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	for _, target := range targets {
		wg.Add(1)
		go func(tgt string) {
			defer wg.Done()
			grpcConn, e := grpc.DialContext(ctx, tgt, opts...)
			if e != nil {
				err = e
				return
			}
			client := api.NewDgraphClient(grpcConn)
			clients = append(clients, client)
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
	cancel             context.CancelFunc
	closed             bool
	username, password string
}

func (d *Client) Txn(readOnly bool) *Txn {
	d.Dgraph.NewTxn()
	if readOnly {
		txn := d.NewReadOnlyTxn()
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
