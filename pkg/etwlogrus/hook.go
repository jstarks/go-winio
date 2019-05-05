package etwlogrus

import (
	"sort"

	"github.com/Microsoft/go-winio/pkg/etw"
	"github.com/sirupsen/logrus"
)

// Hook is a Logrus hook which logs received events to ETW.
type Hook struct {
	// If EventFromMessage is true, then the entry's message is
	// used as the event name.
	EventFromMessage bool

	// EventKey specifies the entry field used to determine the
	// ETW event name. Defaults to "etw.event".
	EventKey string

	// DefaultEvent sets the event name for entries without EventKey.
	// Defaults to "LogrusEntry".
	DefaultEvent string

	// MessageField sets the ETW field name for the entry's message.
	// Only used if EventFromMessage is false. Defaults to "Message".
	MessageField string

	provider      *etw.Provider
	closeProvider bool
}

// NewHook registers a new ETW provider and returns a hook to log from it. The
// provider will be closed when the hook is closed.
func NewHook(providerName string) (*Hook, error) {
	provider, err := etw.NewProvider(providerName, nil)
	if err != nil {
		return nil, err
	}
	h, _ := NewHookFromProvider(provider)
	h.closeProvider = true
	return h, nil
}

// NewHookFromProvider creates a new hook based on an existing ETW provider. The
// provider will not be closed when the hook is closed.
func NewHookFromProvider(provider *etw.Provider) (*Hook, error) {
	return &Hook{
		provider:     provider,
		EventKey:     "etw.event",
		DefaultEvent: "LogrusEntry",
		MessageField: "Message",
	}, nil
}

// Levels returns the set of levels that this hook wants to receive log entries
// for.
func (h *Hook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.TraceLevel,
		logrus.DebugLevel,
		logrus.InfoLevel,
		logrus.WarnLevel,
		logrus.ErrorLevel,
		logrus.FatalLevel,
		logrus.PanicLevel,
	}
}

var logrusToETWLevelMap = map[logrus.Level]etw.Level{
	logrus.PanicLevel: etw.LevelAlways,
	logrus.FatalLevel: etw.LevelCritical,
	logrus.ErrorLevel: etw.LevelError,
	logrus.WarnLevel:  etw.LevelWarning,
	logrus.InfoLevel:  etw.LevelInfo,
	logrus.DebugLevel: etw.LevelVerbose,
	logrus.TraceLevel: etw.LevelVerbose,
}

// Fire receives each Logrus entry as it is logged, and logs it to ETW.
func (h *Hook) Fire(e *logrus.Entry) error {
	// Logrus defines more levels than ETW typically uses, but analysis is
	// easiest when using a consistent set of levels across ETW providers, so we
	// map the Logrus levels to ETW levels.
	level := logrusToETWLevelMap[e.Level]
	if !h.provider.IsEnabledForLevel(level) {
		return nil
	}

	nfields := 0
	eventName := h.DefaultEvent
	if h.EventFromMessage {
		eventName = e.Message
	} else {
		// Reserve extra space for the message field.
		nfields++
	}
	// Sort the fields by name so they are consistent in each instance
	// of an event. Otherwise, the fields don't line up in WPA.
	names := make([]string, 0, len(e.Data))
	var errv interface{}
	for k := range e.Data {
		switch k {
		case h.EventKey:
			if s, ok := e.Data[k].(string); ok {
				eventName = s
			}
		case logrus.ErrorKey:
			// Save the error in order to put it last because
			// some events tend to have this field only sometimes,
			// and it would otherwise mix up the order of fields.
			errv = e.Data[k]
			nfields++
		default:
			names = append(names, k)
			nfields++
		}
	}
	sort.Strings(names)

	fields := make([]etw.FieldOpt, 0, nfields)
	if !h.EventFromMessage {
		fields = append(fields, etw.StringField(h.MessageField, e.Message))
	}
	for _, k := range names {
		fields = append(fields, etw.SmartField(k, e.Data[k]))
	}
	if errv != nil {
		fields = append(fields, etw.SmartField(logrus.ErrorKey, errv))
	}

	return h.provider.WriteEvent(
		eventName,
		etw.WithEventOpts(etw.WithLevel(level)),
		fields)
}

// Close cleans up the hook and closes the ETW provider. If the provder was
// registered by etwlogrus, it will be closed as part of `Close`. If the
// provider was passed in, it will not be closed.
func (h *Hook) Close() error {
	if h.closeProvider {
		return h.provider.Close()
	}
	return nil
}
