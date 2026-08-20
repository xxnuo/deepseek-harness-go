package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
)

type subagentRPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *subagentRPCError) Error() string {
	if len(e.Data) == 0 || string(e.Data) == "null" {
		return fmt.Sprintf("JSON-RPC error %d: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("JSON-RPC error %d: %s (%s)", e.Code, e.Message, e.Data)
}

type subagentRPCFrame struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      json.RawMessage   `json:"id"`
	Method  string            `json:"method"`
	Params  json.RawMessage   `json:"params"`
	Result  json.RawMessage   `json:"result"`
	Error   *subagentRPCError `json:"error"`
}

type subagentRPCResponse struct {
	result json.RawMessage
	err    error
}

type subagentRPCRequestHandler func(string, json.RawMessage) (any, error)
type subagentRPCNotificationHandler func(string, json.RawMessage) error

type subagentRPCClient struct {
	input  io.Reader
	output io.Writer

	writeMu sync.Mutex
	mu      sync.Mutex
	nextID  int64
	pending map[int64]chan subagentRPCResponse
	closed  bool
	err     error

	requests      subagentRPCRequestHandler
	notifications subagentRPCNotificationHandler
	done          chan struct{}
	once          sync.Once
}

func newSubagentRPCClient(input io.Reader, output io.Writer) *subagentRPCClient {
	return &subagentRPCClient{input: input, output: output, pending: map[int64]chan subagentRPCResponse{}, done: make(chan struct{})}
}

func (c *subagentRPCClient) setHandlers(requests subagentRPCRequestHandler, notifications subagentRPCNotificationHandler) {
	c.mu.Lock()
	c.requests = requests
	c.notifications = notifications
	c.mu.Unlock()
}

func (c *subagentRPCClient) start() {
	go c.readLoop()
}

func (c *subagentRPCClient) readLoop() {
	scanner := bufio.NewScanner(c.input)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var frame subagentRPCFrame
		if err := json.Unmarshal(line, &frame); err != nil {
			c.fail(fmt.Errorf("invalid JSON-RPC frame: %w", err))
			return
		}
		if frame.Method != "" {
			if len(frame.ID) == 0 || string(frame.ID) == "null" {
				if err := c.handleNotification(frame.Method, frame.Params); err != nil {
					c.fail(err)
					return
				}
				continue
			}
			if err := c.handleRequest(frame); err != nil {
				c.fail(err)
				return
			}
			continue
		}
		c.handleResponse(frame)
	}
	if err := scanner.Err(); err != nil {
		c.fail(err)
		return
	}
	c.fail(io.EOF)
}

func (c *subagentRPCClient) handleNotification(method string, params json.RawMessage) error {
	c.mu.Lock()
	handler := c.notifications
	c.mu.Unlock()
	if handler == nil {
		return nil
	}
	return handler(method, params)
}

func (c *subagentRPCClient) handleRequest(frame subagentRPCFrame) error {
	c.mu.Lock()
	handler := c.requests
	c.mu.Unlock()
	if handler == nil {
		return c.write(map[string]any{
			"jsonrpc": "2.0", "id": json.RawMessage(frame.ID),
			"error": map[string]any{"code": -32601, "message": "method not found"},
		})
	}
	result, err := handler(frame.Method, frame.Params)
	if err != nil {
		return c.write(map[string]any{
			"jsonrpc": "2.0", "id": json.RawMessage(frame.ID),
			"error": map[string]any{"code": -32603, "message": err.Error()},
		})
	}
	if result == nil {
		result = map[string]any{}
	}
	return c.write(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(frame.ID), "result": result})
}

func (c *subagentRPCClient) handleResponse(frame subagentRPCFrame) {
	id, err := parseSubagentRPCID(frame.ID)
	if err != nil {
		c.fail(err)
		return
	}
	c.mu.Lock()
	waiter := c.pending[id]
	delete(c.pending, id)
	c.mu.Unlock()
	if waiter == nil {
		return
	}
	if frame.Error != nil {
		waiter <- subagentRPCResponse{err: frame.Error}
	} else {
		waiter <- subagentRPCResponse{result: append(json.RawMessage(nil), frame.Result...)}
	}
}

func parseSubagentRPCID(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 {
		return 0, errors.New("JSON-RPC response omitted id")
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil {
		id, intErr := strconv.ParseInt(number.String(), 10, 64)
		if intErr == nil {
			return id, nil
		}
	}
	return 0, fmt.Errorf("JSON-RPC response carried invalid id: %s", raw)
}

func (c *subagentRPCClient) request(ctx context.Context, method string, params any, target any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if c.closed {
		err := c.err
		c.mu.Unlock()
		if err == nil {
			err = io.EOF
		}
		return err
	}
	c.nextID++
	id := c.nextID
	waiter := make(chan subagentRPCResponse, 1)
	c.pending[id] = waiter
	c.mu.Unlock()
	if err := c.write(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		c.removePending(id)
		return err
	}
	select {
	case <-ctx.Done():
		c.removePending(id)
		return ctx.Err()
	case response := <-waiter:
		if response.err != nil {
			return response.err
		}
		if target == nil {
			return nil
		}
		if len(response.result) == 0 {
			response.result = json.RawMessage(`null`)
		}
		if err := json.Unmarshal(response.result, target); err != nil {
			return fmt.Errorf("%s returned invalid result: %w", method, err)
		}
		return nil
	}
}

func (c *subagentRPCClient) notify(method string, params any) error {
	return c.write(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}

func (c *subagentRPCClient) removePending(id int64) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *subagentRPCClient) write(value any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	closed, terminal := c.closed, c.err
	c.mu.Unlock()
	if closed {
		if terminal != nil {
			return terminal
		}
		return io.ErrClosedPipe
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = c.output.Write(data)
	if err != nil {
		c.fail(err)
	}
	return err
}

func (c *subagentRPCClient) fail(err error) {
	if err == nil {
		err = io.EOF
	}
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.err = err
		pending := c.pending
		c.pending = map[int64]chan subagentRPCResponse{}
		c.mu.Unlock()
		for _, waiter := range pending {
			waiter <- subagentRPCResponse{err: err}
		}
		close(c.done)
	})
}

func (c *subagentRPCClient) close() { c.fail(io.ErrClosedPipe) }

func (c *subagentRPCClient) waitError() error {
	<-c.done
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

func decodeSubagentRPCParams(raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	return json.Unmarshal(raw, target)
}
