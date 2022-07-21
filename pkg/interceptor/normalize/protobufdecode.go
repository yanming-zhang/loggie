package normalize

import (
	"time"

	proto "github.com/gogo/protobuf/proto"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/protoLogEvent"
	"github.com/loggie-io/loggie/pkg/util/runtime"
)

const ProcessorProtobufDecode = "protobufDecode"

type ProtobufDecodeProcessor struct {
	config      *ProtobufDecodeConfig
	interceptor *Interceptor
}

type ProtobufDecodeConfig struct {
	Target      string `yaml:"target,omitempty" default:"body"`
	IgnoreError bool   `yaml:"ignoreError"`
}

func init() {
	register(ProcessorProtobufDecode, func() Processor {
		return NewProtobufDecodeProcessor()
	})
}

func NewProtobufDecodeProcessor() *ProtobufDecodeProcessor {
	return &ProtobufDecodeProcessor{
		config: &ProtobufDecodeConfig{},
	}
}

func (r *ProtobufDecodeProcessor) Config() interface{} {
	return r.config
}

func (r *ProtobufDecodeProcessor) Init(interceptor *Interceptor) {
	r.interceptor = interceptor
}

func (r *ProtobufDecodeProcessor) GetName() string {
	return ProcessorProtobufDecode
}

func (r *ProtobufDecodeProcessor) Process(e api.Event) error {
	if r.config == nil {
		return nil
	}

	header := e.Header()
	if header == nil {
		header = make(map[string]interface{})
	}

	var val []byte
	target := r.config.Target
	if target == event.Body {
		val = e.Body()
	} else {
		obj := runtime.NewObject(header)
		v, err := obj.GetPath(target).String()
		if err != nil {
			LogErrorWithIgnore(r.config.IgnoreError, "get content from %s failed %v", target, err)
			r.interceptor.reportMetric(r)
			return nil
		}
		if v == "" {
			return nil
		}

		val = []byte(v)
	}

	res := make(map[string]interface{})
	ple := protoLogEvent.ProtoLogEvent{}
	err := proto.Unmarshal(val, &ple)
	if err != nil {
		LogErrorWithIgnore(r.config.IgnoreError, "unmarshal data: %s err: %v", string(val), err)
		r.interceptor.reportMetric(r)
		return nil
	}

	res = map[string]interface{}{
		"@timestamp":      time.Now(),
		"loggerFqcn":      ple.LoggerFqcn,
		"marker":          ple.Marker,
		"level":           ple.Level,
		"loggerName":      ple.LoggerName,
		"message":         ple.Message,
		"timeMillis":      ple.TimeMillis,
		"thrown":          ple.Thrown,
		"thrownProxy":     ple.ThrownProxy,
		"contextMap":      ple.ContextMap,
		"contextStack":    ple.ContextStack,
		"threadName":      ple.ThreadName,
		"source":          ple.Source,
		"includeLocation": ple.IncludeLocation,
		"endOfBatch":      ple.EndOfBatch,
		"containerMeta":   ple.ContainerMeta,
		"appname":         ple.ContainerMeta.AppName,
		"procname":        ple.ContainerMeta.ProcName,
		"k8s_pod":         ple.ContainerMeta.ContainerId,
		"nanoTime":        ple.NanoTime,
	}

	for k, v := range res {
		header[k] = v
	}

	return nil
}
