package client

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	"github.com/dhcmrlchtdj/raftoy/rpc"
)

type Client struct {
	cluster     []string
	client      rpc.RaftRpcClient
	clientId    uint64
	sequenceNum uint64
}

func New(cluster []string) *Client {
	c := Client{
		cluster:     cluster,
		client:      nil,
		clientId:    0,
		sequenceNum: 0,
	}
	return &c
}

func (c *Client) tryConnect(server string) (uint64, rpc.RaftRpcClient, error) {
	conn, err := grpc.Dial(server, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return 0, nil, err
	}
	client := rpc.NewRaftRpcClient(conn)
	resp, err := client.RegisterClient(context.Background(), new(rpc.ReqRegisterClient))
	if err != nil {
		return 0, nil, err
	}
	if resp.Status == rpc.RespRegisterClient_OK {
		return *resp.ClientId, client, nil
	}
	// if resp.LeaderHint != nil && len(*resp.LeaderHint) > 0 {
	//     return c.tryConnect(*resp.LeaderHint)
	// }
	return 0, nil, errors.New("can't connect")
}

func (c *Client) Conn() {
	for _, server := range c.cluster {
		id, client, err := c.tryConnect(server)
		if err != nil {
			continue
		}
		c.clientId = id
		c.client = client
		return
	}
	panic("can't connect to cluster")
}

func (c *Client) Get(key string) *string {
	maxRetry := 2
	for maxRetry > 0 {
		maxRetry--
		resp, err := c.client.ClientQuery(context.Background(), &rpc.ReqClientQuery{Query: key})
		if err != nil {
			panic(err)
		}
		if resp.Status == rpc.RespClient_OK {
			return resp.Response
		} else {
			c.Conn()
		}
	}
	panic("can't connect to cluster")
}

func (c *Client) clientRequest(cmd string) {
	maxRetry := 2
	for maxRetry > 0 {
		maxRetry--
		resp, err := c.client.ClientRequest(context.Background(),
			&rpc.ReqClientRequest{
				ClientId:    c.clientId,
				SequenceNum: c.sequenceNum, // FIXME
				Command:     cmd,
			})
		if err != nil {
			panic(err)
		}
		if resp.Status == rpc.RespClient_OK {
			return
		} else {
			c.Conn()
		}
	}
	panic("can't connect to cluster")
}

func (c *Client) Set(key string, val string) {
	c.clientRequest("set:" + key + ":" + val)
}

func (c *Client) Del(key string) {
	c.clientRequest("del:" + key)
}
