package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/fsouza/go-dockerclient"
	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

func debug(v ...interface{}) {
	if os.Getenv("DEBUG") != "" {
		log.Println(v...)
	}
}

var K8S_POD_UID_LABEL = "io.kubernetes.pod.uid"
var K8S_POD_TYPE_LABEL = "io.kubernetes.docker.type"
var K8S_POD_PARENT_TYPE = "podsandbox"
var K8S_POD_CONTAINER_TYPE = "container"
var K8S_IO_PREFIX = "io.kubernetes."
var K8S_ANNOTATION_PREFIX = "annotation.kubernetes.io/"

type DockerClient interface {
	CreateContainer(docker.CreateContainerOptions) (*docker.Container, error)
	ListContainers(docker.ListContainersOptions) ([]docker.APIContainers, error)
	Info() (*docker.DockerInfo, error)
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn           net.Conn
	route          *router.Route
	containerTags  map[string][]string
	logstashFields map[string]map[string]string
	decodeJsonLogs map[string]bool
	k8sLabels      map[string]map[string]string
	client         DockerClient
}

// NewLogstashAdapter creates a LogstashAdapter with UDP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	for {
		client, err := docker.NewClientFromEnv()
		if err != nil {
			return nil, errors.New("cannot create docker client: " + err.Error())
		}

		conn, err := transport.Dial(route.Address, route.Options)

		if err == nil {
			return &LogstashAdapter{
				route:          route,
				conn:           conn,
				containerTags:  make(map[string][]string),
				logstashFields: make(map[string]map[string]string),
				decodeJsonLogs: make(map[string]bool),
				k8sLabels:      make(map[string]map[string]string),
				client:         client,
			}, nil
		}
		if os.Getenv("RETRY_STARTUP") == "" {
			return nil, err
		}
		log.Println("Retrying:", err)
		time.Sleep(2 * time.Second)
	}
}

// Get container tags configured with the environment variable LOGSTASH_TAGS
func GetContainerTags(c *docker.Container, a *LogstashAdapter) []string {
	if tags, ok := a.containerTags[c.ID]; ok {
		return tags
	}

	tags := []string{}
	tagsStr := os.Getenv("LOGSTASH_TAGS")

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_TAGS=") {
			tagsStr = strings.TrimPrefix(e, "LOGSTASH_TAGS=")
			break
		}
	}

	if len(tagsStr) > 0 {
		tags = strings.Split(tagsStr, ",")
	}

	a.containerTags[c.ID] = tags
	return tags
}

// Get logstash fields configured with the environment variable LOGSTASH_FIELDS
func GetLogstashFields(c *docker.Container, a *LogstashAdapter) map[string]string {
	if fields, ok := a.logstashFields[c.ID]; ok {
		return fields
	}

	fieldsStr := os.Getenv("LOGSTASH_FIELDS")
	fields := map[string]string{}

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_FIELDS=") {
			fieldsStr = strings.TrimPrefix(e, "LOGSTASH_FIELDS=")
		}
	}

	if len(fieldsStr) > 0 {
		for _, f := range strings.Split(fieldsStr, ",") {
			sp := strings.Split(f, "=")
			k, v := sp[0], sp[1]
			fields[k] = v
		}
	}

	a.logstashFields[c.ID] = fields

	return fields
}

func SelectContainerLabels(source map[string]string) map[string]string {
	result := make(map[string]string)

	for k, v := range source {
		if strings.HasPrefix(k, K8S_IO_PREFIX) || strings.HasPrefix(k, K8S_ANNOTATION_PREFIX) {
			continue
		}

		result[k] = v
	}

	return result
}

func Merge(m1, m2 map[string]string) map[string]string {
	for i, v := range m1 {
		if _, ok := m2[i]; !ok {
			m2[i] = v
		} else {
			j := "pod_" + i
			m2[j] = v
		}
	}
	return m2
}

func GetDockerLabels(a *LogstashAdapter) map[string]string {
	info, err := a.client.Info()
	if err != nil {
		log.Print("Cannot get Docker info: ", err)
		return nil
	}

	labels := map[string]string{
		"host":           info.Name,
		"docker_version": info.ServerVersion,
	}
	return labels
}

func GetPodLabels(c *docker.Container, current_labels map[string]string, a *LogstashAdapter) (map[string]string, error) {
	if labels, ok := a.k8sLabels[c.ID]; ok {
		debug("Got labels already for container %s", c.ID)
		return labels, nil
	}

	debug("Looking for labels for container %s for the first time", c.ID)

	// only mutate if the pod uid label exists (it's not an error if the label doesn't exist)
	if _, ok := c.Config.Labels[K8S_POD_UID_LABEL]; !ok {
		debug("There are no K8S labels for container %s", c.ID)
		return current_labels, nil
	}

	debug("Container %s is in a K8S pod", c.ID)

	// find parent container
	fltr := K8S_POD_UID_LABEL + "=" + c.Config.Labels[K8S_POD_UID_LABEL]
	opts := docker.ListContainersOptions{
		Filters: map[string][]string{"label": {fltr}},
	}
	containers, err := a.client.ListContainers(opts)
	if err != nil {
		return nil, err
	}

	debug("Got some containers to check: %v", containers)

	for _, ctr := range containers {
		if ctr.Labels[K8S_POD_UID_LABEL] == c.Config.Labels[K8S_POD_UID_LABEL] && ctr.Labels[K8S_POD_TYPE_LABEL] == K8S_POD_PARENT_TYPE {
			debug("Container %s is a pod leader", ctr.ID)
			a.k8sLabels[c.ID] = Merge(SelectContainerLabels(ctr.Labels), current_labels)
			a.k8sLabels[c.ID] = Merge(GetDockerLabels(a), a.k8sLabels[c.ID])
			debug("Returning labels: %v\n", a.k8sLabels[c.ID])
			return a.k8sLabels[c.ID], nil
		} else {
			debug("Container %s is not a pod leader", ctr.ID)
		}
	}

	debug("Returning current_labels %v -- could not find a container to match", current_labels)

	return current_labels, nil
}

// Get boolean indicating whether json logs should be decoded (or added as message),
// configured with the environment variable DECODE_JSON_LOGS
func IsDecodeJsonLogs(c *docker.Container, a *LogstashAdapter) bool {
	if decodeJsonLogs, ok := a.decodeJsonLogs[c.ID]; ok {
		return decodeJsonLogs
	}

	decodeJsonLogsStr := os.Getenv("DECODE_JSON_LOGS")

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "DECODE_JSON_LOGS=") {
			decodeJsonLogsStr = strings.TrimPrefix(e, "DECODE_JSON_LOGS=")
		}
	}

	decodeJsonLogs := decodeJsonLogsStr != "false"

	a.decodeJsonLogs[c.ID] = decodeJsonLogs

	return decodeJsonLogs
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {

	for m := range logstream {

		dockerInfo := DockerInfo{
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
		}

		if os.Getenv("DOCKER_LABELS") != "" {
			labels := make(map[string]string)
			for label, value := range m.Container.Config.Labels {
				labels[strings.Replace(label, ".", "_", -1)] = value
			}

			labels, err := GetPodLabels(m.Container, labels, a)
			if err != nil {
				log.Fatal("Could not get pod labels: ", err)
			}

			dockerInfo.Labels = labels
		}

		tags := GetContainerTags(m.Container, a)
		fields := GetLogstashFields(m.Container, a)
		decodeJson := IsDecodeJsonLogs(m.Container, a)

		// For some Docker versions (18.6, 18.9 at least), the journald
		// driver doesn't separate long messages properly, and you get two log
		// events concatenated with a single carriage return
		if os.Getenv("BROKEN_JOURNALD") != "" {
			if strings.Index(m.Data, "\r") < 0 {
				a.sendMessage(m.Source, m.Data, dockerInfo, tags, fields, decodeJson)
			} else {
				for _, msg := range strings.Split(m.Data, "\r") {
					a.sendMessage(m.Source, msg, dockerInfo, tags, fields, decodeJson)
				}
			}
		}
	}
}

func (a *LogstashAdapter) sendMessage(source, message string, dockerInfo DockerInfo, tags []string, fields map[string]string, decodeJson bool) {
	var js []byte
	var data map[string]interface{}
	var err error

	// Try to parse JSON-encoded m.Data. If it wasn't JSON, create an empty object
	// and use the original data as the message.
	if decodeJson {
		err = json.Unmarshal([]byte(message), &data)
	}
	if err != nil || data == nil {
		data = make(map[string]interface{})
		data["message"] = message
	}

	for k, v := range fields {
		data[k] = v
	}

	data["docker"] = dockerInfo
	data["stream"] = source
	data["tags"] = tags

	// Return the JSON encoding
	if js, err = json.Marshal(data); err != nil {
		// Log error message and continue parsing next line, if marshalling fails
		log.Println("logstash: could not marshal JSON:", err)
		return
	}

	// To work with tls and tcp transports via json_lines codec
	js = append(js, byte('\n'))

	for {
		_, err := a.conn.Write(js)

		if err == nil {
			break
		}

		if os.Getenv("RETRY_SEND") == "" {
			log.Fatal("logstash: could not write:", err)
		} else {
			time.Sleep(2 * time.Second)
		}
	}
}

type DockerInfo struct {
	Name     string            `json:"name"`
	ID       string            `json:"id"`
	Image    string            `json:"image"`
	Hostname string            `json:"hostname"`
	Labels   map[string]string `json:"labels"`
}
