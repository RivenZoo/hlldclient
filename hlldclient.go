package hlldClient

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

var (
	DONE     = []byte("Done\n")
	START    = []byte("START\n")
	END      = []byte("END\n")
	EXISTS   = []byte("Exists\n")
	DELETING = []byte("Delete in progress\n")
)

var (
	ErrKeyExists           = errors.New("key exists")
	ErrKeyDeleteInProgress = errors.New("key deleted in progress")
)

type HlldClient struct {
	conn net.Conn
}

type SetAttr struct {
	Precision int
	Eps       float32
	InMemory  bool
}

func (sa *SetAttr) attrToArgs() string {
	args := []string{}
	if sa.Precision >= 4 && sa.Precision <= 18 {
		args = append(args, fmt.Sprintf("precision=%d", sa.Precision))
	}
	if sa.Eps != 0.0 {
		args = append(args, fmt.Sprintf("eps=%f", sa.Eps))
	}
	if sa.InMemory {
		args = append(args, "in_memory=1")
	}
	if len(args) != 0 {
		return strings.Join(args, " ")
	}
	return ""
}

func NewHlldClient(addr string) *HlldClient {
	c := &HlldClient{}
	conn, err := net.DialTimeout("tcp", addr, time.Second*10)
	if err != nil {
		panic(err)
	}
	c.conn = conn
	return c
}

func (c *HlldClient) Close() {
	c.conn.Close()
}

func (c *HlldClient) do(cmd, key string, args ...string) ([]byte, error) {
	argstr := fmt.Sprintf("%s", args)
	argstr = argstr[1 : len(argstr)-1]
	var msg string
	if len(argstr) != 0 {
		msg = fmt.Sprintf("%s %s %s\n", cmd, key, argstr)
	} else {
		msg = fmt.Sprintf("%s %s\n", cmd, key)
	}

	c.conn.SetDeadline(time.Now().Add(time.Second * 5))
	io.WriteString(c.conn, msg)

	buf := make([]byte, 128)
	n, err := c.conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (c *HlldClient) Create(key string, attr *SetAttr) error {
	attrstr := ""
	if attr != nil {
		attrstr = attr.attrToArgs()
	}
	b, err := c.do("create", key, attrstr)
	if err != nil {
		return err
	}
	if bytes.Compare(DONE, b) != 0 {
		if bytes.Compare(DELETING, b) == 0 {
			return ErrKeyDeleteInProgress
		} else {
			return ErrKeyExists
		}
	}
	return nil
}

func (c *HlldClient) List(key string) (int64, error) {
	b, err := c.do("list", key)
	if err != nil {
		return 0, err
	}
	start := bytes.Index(b, START)
	end := bytes.Index(b, END)
	if start == -1 || end == -1 {
		return 0, errors.New(string(b))
	}
	info := b[start+6 : end]
	info = bytes.Trim(info, " \n")
	parts := bytes.Split(info, []byte(" "))

	if len(parts) != 5 {
		return 0, errors.New("no key info")
	}
	count, err := strconv.ParseInt(string(parts[4]), 10, 64)
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	return count, nil
}

func (c *HlldClient) Set(key, val string) error {
	b, err := c.do("set", key, val)
	if err != nil {
		return err
	}
	if bytes.Compare(DONE, b) != 0 {
		return errors.New(string(b))
	}
	return nil
}

func (c *HlldClient) Bulk(key string, args ...string) error {
	b, err := c.do("bulk", key, args...)
	if err != nil {
		return err
	}
	if bytes.Compare(DONE, b) != 0 {
		return errors.New(string(b))
	}
	return nil
}

func (c *HlldClient) Drop(key string) error {
	b, err := c.do("drop", key)
	if err != nil {
		return err
	}
	if bytes.Compare(DONE, b) != 0 {
		return errors.New(string(b))
	}
	return nil
}
