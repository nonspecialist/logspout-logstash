package logstash

import (
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/fsouza/go-dockerclient"
	"github.com/gliderlabs/logspout/router"
	"github.com/stretchr/testify/assert"
)

var res string

type MockConn struct {
}

func (m MockConn) Close() error {
	return nil
}

func (m MockConn) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (m MockConn) Write(b []byte) (n int, err error) {
	res = string(b)
	return 0, nil
}

func (m MockConn) LocalAddr() net.Addr {
	return nil
}

func (m MockConn) RemoteAddr() net.Addr {
	return nil
}

func (m MockConn) SetDeadline(t time.Time) error {
	return nil
}

func (m MockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m MockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

type MockClient struct {
	containers []*docker.Container
}

func (m *MockClient) CreateContainer(opts docker.CreateContainerOptions) (*docker.Container, error) {
	ctr := &docker.Container{
		ID:     "deadbeef",
		Name:   opts.Name,
		Config: opts.Config,
	}
	m.containers = append(m.containers, ctr)
	return ctr, nil
}

func (m *MockClient) ListContainers(opts docker.ListContainersOptions) ([]docker.APIContainers, error) {
	var containers []docker.APIContainers
	for _, c := range m.containers {
		x := docker.APIContainers{
			ID:     c.ID,
			Image:  c.Image,
			Labels: c.Config.Labels,
		}
		containers = append(containers, x)
	}
	return containers, nil
}

func (m *MockClient) Info() (*docker.DockerInfo, error) {
	result := &docker.DockerInfo{
		Name:          "banana-potato",
		ServerVersion: "tangerine",
	}
	return result, nil
}

func TestStreamNullData(t *testing.T) {
	assert := assert.New(t)

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash", "LOGSTASH_TAGS=example,tags", "MORE_NON_LOGSTASH_TAGS=dont,include"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `null`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("null", data["message"])
	assert.Equal([]interface{}{"example", "tags"}, data["tags"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamNotJsonWithoutLogstashTags(t *testing.T) {
	assert := assert.New(t)

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `foo bananas`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("foo bananas", data["message"])
	assert.Equal([]interface{}{}, data["tags"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamNotJsonWithLogstashTags(t *testing.T) {
	assert := assert.New(t)

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash", "LOGSTASH_TAGS=example,tags", "MORE_NON_LOGSTASH_TAGS=dont,include"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `foo bananas`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("foo bananas", data["message"])
	assert.Equal([]interface{}{"example", "tags"}, data["tags"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamJsonWithoutLogstashTags(t *testing.T) {
	assert := assert.New(t)

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `{ "remote_user": "-", "body_bytes_sent": "25", "request_time": "0.821", "status": "200", "request_method": "POST", "http_referrer": "-", "http_user_agent": "-" }`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("-", data["remote_user"])
	assert.Equal("25", data["body_bytes_sent"])
	assert.Equal("0.821", data["request_time"])
	assert.Equal("200", data["status"])
	assert.Equal("POST", data["request_method"])
	assert.Equal("-", data["http_referrer"])
	assert.Equal("-", data["http_user_agent"])
	assert.Equal([]interface{}{}, data["tags"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamJsonWithLogstashTags(t *testing.T) {
	assert := assert.New(t)

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"LOGSTASH_TAGS=example,tags", "NON_LOGSTASH_TAGS=not,logstash"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `{ "remote_user": "-", "body_bytes_sent": "25", "request_time": "0.821", "status": "200", "request_method": "POST", "http_referrer": "-", "http_user_agent": "-" }`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("-", data["remote_user"])
	assert.Equal("25", data["body_bytes_sent"])
	assert.Equal("0.821", data["request_time"])
	assert.Equal("200", data["status"])
	assert.Equal("POST", data["request_method"])
	assert.Equal("-", data["http_referrer"])
	assert.Equal("-", data["http_user_agent"])
	assert.Equal([]interface{}{"example", "tags"}, data["tags"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamNotJsonWithLogstashFields(t *testing.T) {
	assert := assert.New(t)

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash", "LOGSTASH_FIELDS=myfield=something,anotherfield=something_else", "MORE_NON_LOGSTASH_TAGS=dont,include"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `foo bananas`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("foo bananas", data["message"])
	assert.Equal([]interface{}{}, data["tags"])
	assert.Equal("something", data["myfield"])
	assert.Equal("something_else", data["anotherfield"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamJsonWithLogstashFields(t *testing.T) {
	assert := assert.New(t)

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash", "LOGSTASH_FIELDS=myfield=something,anotherfield=something_else", "MORE_NON_LOGSTASH_TAGS=dont,include"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `{ "remote_user": "-", "body_bytes_sent": "25", "request_time": "0.821", "status": "200", "request_method": "POST", "http_referrer": "-", "http_user_agent": "-" }`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("-", data["remote_user"])
	assert.Equal("25", data["body_bytes_sent"])
	assert.Equal("0.821", data["request_time"])
	assert.Equal("200", data["status"])
	assert.Equal("POST", data["request_method"])
	assert.Equal("-", data["http_referrer"])
	assert.Equal("-", data["http_user_agent"])
	assert.Equal([]interface{}{}, data["tags"])
	assert.Equal("something", data["myfield"])
	assert.Equal("something_else", data["anotherfield"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamNotJsonWithLogstashFieldsWithDefault(t *testing.T) {
	assert := assert.New(t)

	os.Setenv("LOGSTASH_FIELDS", "myfield=something,anotherfield=something_else")

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash", "MORE_NON_LOGSTASH_TAGS=dont,include"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `foo bananas`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("foo bananas", data["message"])
	assert.Equal([]interface{}{}, data["tags"])
	assert.Equal("something", data["myfield"])
	assert.Equal("something_else", data["anotherfield"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamJsonWithLogstashFieldsWithDefault(t *testing.T) {
	assert := assert.New(t)

	os.Setenv("LOGSTASH_FIELDS", "myfield=something,anotherfield=something_else")

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash", "MORE_NON_LOGSTASH_TAGS=dont,include"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `{ "remote_user": "-", "body_bytes_sent": "25", "request_time": "0.821", "status": "200", "request_method": "POST", "http_referrer": "-", "http_user_agent": "-" }`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("-", data["remote_user"])
	assert.Equal("25", data["body_bytes_sent"])
	assert.Equal("0.821", data["request_time"])
	assert.Equal("200", data["status"])
	assert.Equal("POST", data["request_method"])
	assert.Equal("-", data["http_referrer"])
	assert.Equal("-", data["http_user_agent"])
	assert.Equal([]interface{}{}, data["tags"])
	assert.Equal("something", data["myfield"])
	assert.Equal("something_else", data["anotherfield"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamNotJsonWithLogstashTagsWithDefault(t *testing.T) {
	assert := assert.New(t)

	os.Setenv("LOGSTASH_TAGS", "example,tags")

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash", "MORE_NON_LOGSTASH_TAGS=dont,include"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `foo bananas`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("foo bananas", data["message"])
	assert.Equal([]interface{}{"example", "tags"}, data["tags"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamJsonWithLogstashTagsWithDefault(t *testing.T) {
	assert := assert.New(t)

	os.Setenv("LOGSTASH_TAGS", "example,tags")

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `{ "remote_user": "-", "body_bytes_sent": "25", "request_time": "0.821", "status": "200", "request_method": "POST", "http_referrer": "-", "http_user_agent": "-" }`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("-", data["remote_user"])
	assert.Equal("25", data["body_bytes_sent"])
	assert.Equal("0.821", data["request_time"])
	assert.Equal("200", data["status"])
	assert.Equal("POST", data["request_method"])
	assert.Equal("-", data["http_referrer"])
	assert.Equal("-", data["http_user_agent"])
	assert.Equal([]interface{}{"example", "tags"}, data["tags"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamJsonWithLogstashFieldsAndBlacklist(t *testing.T) {
	assert := assert.New(t)

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"LOGSTASH_FIELDS=myfield=something,anotherfield=something_else,tags=nastytag,docker=cheating", "LOGSTASH_TAGS=mytag,anothertag"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `{ "remote_user": "-", "body_bytes_sent": "25", "request_time": "0.821", "status": "200", "request_method": "POST", "http_referrer": "-", "http_user_agent": "-" }`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("-", data["remote_user"])
	assert.Equal("25", data["body_bytes_sent"])
	assert.Equal("0.821", data["request_time"])
	assert.Equal("200", data["status"])
	assert.Equal("POST", data["request_method"])
	assert.Equal("-", data["http_referrer"])
	assert.Equal("-", data["http_user_agent"])
	assert.Equal([]interface{}{"mytag", "anothertag"}, data["tags"])
	assert.Equal("something", data["myfield"])
	assert.Equal("something_else", data["anotherfield"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamJsonWithLogstashFieldsWithDefaultAndBlacklist(t *testing.T) {
	assert := assert.New(t)

	os.Setenv("LOGSTASH_FIELDS", "myfield=something,anotherfield=something_else,tags=nastytag,docker=cheating")
	os.Setenv("LOGSTASH_TAGS", "nicetag,righttag")

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash", "MORE_NON_LOGSTASH_TAGS=dont,include"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `{ "remote_user": "-", "body_bytes_sent": "25", "request_time": "0.821", "status": "200", "request_method": "POST", "http_referrer": "-", "http_user_agent": "-" }`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("-", data["remote_user"])
	assert.Equal("25", data["body_bytes_sent"])
	assert.Equal("0.821", data["request_time"])
	assert.Equal("200", data["status"])
	assert.Equal("POST", data["request_method"])
	assert.Equal("-", data["http_referrer"])
	assert.Equal("-", data["http_user_agent"])
	assert.Equal([]interface{}{"nicetag", "righttag"}, data["tags"])
	assert.Equal("something", data["myfield"])
	assert.Equal("something_else", data["anotherfield"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamJsonLabelsDisabled(t *testing.T) {
	assert := assert.New(t)

	os.Setenv("LOGSTASH_FIELDS", "")
	os.Setenv("LOGSTASH_TAGS", "")
	os.Setenv("DOCKER_LABELS", "")

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash"}
	containerConfig.Labels = map[string]string{"ignore": "this"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `{ "remote_user": "-", "body_bytes_sent": "25", "request_time": "0.821", "status": "200", "request_method": "POST", "http_referrer": "-", "http_user_agent": "-" }`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("-", data["remote_user"])
	assert.Equal("25", data["body_bytes_sent"])
	assert.Equal("0.821", data["request_time"])
	assert.Equal("200", data["status"])
	assert.Equal("POST", data["request_method"])
	assert.Equal("-", data["http_referrer"])
	assert.Equal("-", data["http_user_agent"])
	assert.Equal([]interface{}{}, data["tags"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
	assert.Nil(dockerInfo["labels"])
}

func TestStreamJsonLabelsEnabled(t *testing.T) {
	assert := assert.New(t)

	os.Setenv("LOGSTASH_FIELDS", "")
	os.Setenv("LOGSTASH_TAGS", "")
	os.Setenv("DOCKER_LABELS", "1")

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash"}
	containerConfig.Labels = map[string]string{"log": "this", "another.label": "with.dots"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `{ "remote_user": "-", "body_bytes_sent": "25", "request_time": "0.821", "status": "200", "request_method": "POST", "http_referrer": "-", "http_user_agent": "-" }`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("-", data["remote_user"])
	assert.Equal("25", data["body_bytes_sent"])
	assert.Equal("0.821", data["request_time"])
	assert.Equal("200", data["status"])
	assert.Equal("POST", data["request_method"])
	assert.Equal("-", data["http_referrer"])
	assert.Equal("-", data["http_user_agent"])
	assert.Equal([]interface{}{}, data["tags"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
	assert.NotNil(dockerInfo["labels"])

	dockerLabels, ok := dockerInfo["labels"].(map[string]interface{})

	assert.Equal(true, ok)
	assert.Equal("this", dockerLabels["log"])
	assert.Equal("with.dots", dockerLabels["another_label"])
	assert.Nil(dockerLabels["another.label"])
}

func TestStreamJsonLabelsEnabledButEmpty(t *testing.T) {
	assert := assert.New(t)

	os.Setenv("LOGSTASH_FIELDS", "")
	os.Setenv("LOGSTASH_TAGS", "")
	os.Setenv("DOCKER_LABELS", "1")

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash"}
	containerConfig.Labels = map[string]string{}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `{ "remote_user": "-", "body_bytes_sent": "25", "request_time": "0.821", "status": "200", "request_method": "POST", "http_referrer": "-", "http_user_agent": "-" }`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("-", data["remote_user"])
	assert.Equal("25", data["body_bytes_sent"])
	assert.Equal("0.821", data["request_time"])
	assert.Equal("200", data["status"])
	assert.Equal("POST", data["request_method"])
	assert.Equal("-", data["http_referrer"])
	assert.Equal("-", data["http_user_agent"])
	assert.Equal([]interface{}{}, data["tags"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
	assert.NotNil(dockerInfo["labels"])

	dockerLabels, ok := dockerInfo["labels"].(map[string]interface{})

	assert.Equal(true, ok)
	assert.Nil(dockerLabels["log"])
}

func TestStreamJsonWithDecodeJsonLogsFalse(t *testing.T) {
	assert := assert.New(t)

	conn := MockConn{}

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"NON_LOGSTASH_TAGS=not,logstash", "DECODE_JSON_LOGS=false"}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `{ "remote_user": "-", "body_bytes_sent": "25", "request_time": "0.821", "status": "200", "request_method": "POST", "http_referrer": "-", "http_user_agent": "-" }`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal(str, data["message"])
	assert.Equal([]interface{}{}, data["tags"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})
	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])
}

func TestStreamK8SPodNames(t *testing.T) {
	assert := assert.New(t)

	conn := MockConn{}
	client := MockClient{}

	otherContainerConfig := docker.Config{}
	otherContainerConfig.Image = "otherImage"
	otherContainerConfig.Hostname = "otherHostname"
	otherContainerConfig.Env = []string{"PATH=/bin"}
	otherContainerConfig.Labels = map[string]string{
		"io.kubernetes.pod.uid":        "POD-UUID",
		"io.kubernetes.container.name": "potato",
		"io.kubernetes.docker.type":    "container",
		"original":                     "plobble-goo",
	}

	otherContainerOpts := docker.CreateContainerOptions{
		Name:   "otherName",
		Config: &otherContainerConfig,
	}

	podConfig := docker.Config{}
	podConfig.Image = "pauseImage"
	podConfig.Hostname = "podParent"
	podConfig.Env = []string{"PATH=/bin"}
	podConfig.Labels = map[string]string{
		"io.kubernetes.pod.uid":        "POD-UUID",
		"io.kubernetes.docker.type":    "podsandbox",
		"io.kubernetes.container.name": "POD",
		"app":     "myapp",
		"thingy":  "thangy",
		"release": "bibbling-trouser",
	}

	parentContainerOpts := docker.CreateContainerOptions{
		Name:   "podParent",
		Config: &podConfig,
	}

	client.CreateContainer(otherContainerOpts)
	client.CreateContainer(parentContainerOpts)

	adapter := LogstashAdapter{
		route:          new(router.Route),
		conn:           conn,
		containerTags:  make(map[string][]string),
		logstashFields: make(map[string]map[string]string),
		decodeJsonLogs: make(map[string]bool),
		k8sLabels:      make(map[string]map[string]string),
		client:         &client,
	}

	assert.NotNil(adapter)

	logstream := make(chan *router.Message)

	containerConfig := docker.Config{}
	containerConfig.Image = "image"
	containerConfig.Hostname = "hostname"
	containerConfig.Env = []string{"PATH=/bin"}
	containerConfig.Labels = map[string]string{
		"io.kubernetes.pod.uid":        "POD-UUID",
		"io.kubernetes.container.name": "eggplant",
		"io.kubernetes.docker.type":    "container",
		"original":                     "original",
		"release":                      "12345",
	}

	container := docker.Container{}
	container.Name = "name"
	container.ID = "ID"
	container.Config = &containerConfig

	str := `foo bananas`

	message := router.Message{
		Container: &container,
		Source:    "FOOOOO",
		Data:      str,
		Time:      time.Now(),
	}

	go func() {
		logstream <- &message
		close(logstream)
	}()

	adapter.Stream(logstream)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(res), &data)
	assert.Nil(err)

	assert.Equal("foo bananas", data["message"])

	var dockerInfo map[string]interface{}
	dockerInfo = data["docker"].(map[string]interface{})

	assert.Equal("name", dockerInfo["name"])
	assert.Equal("ID", dockerInfo["id"])
	assert.Equal("image", dockerInfo["image"])
	assert.Equal("hostname", dockerInfo["hostname"])

	var labels map[string]interface{}
	labels = dockerInfo["labels"].(map[string]interface{})

	assert.Equal("myapp", labels["app"])
	assert.Equal("POD-UUID", labels["io_kubernetes_pod_uid"])
	assert.Equal("original", labels["original"])
	assert.Equal("12345", labels["release"])
	assert.Equal("bibbling-trouser", labels["pod_release"])

	assert.Equal("banana-potato", labels["host"])
	assert.Equal("tangerine", labels["docker_version"])
}
