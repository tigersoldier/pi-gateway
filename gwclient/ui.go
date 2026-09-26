package gwclient

import "encoding/json"

// Extension dialogs (docs/rpc.md, Extension UI Requests). pi blocks on the
// four dialog methods until the client answers with RespondUI; the other
// methods are notifications the client may display or ignore.

// UI method names carried on extension_ui_request.
const (
	UIMethodSelect        = "select"
	UIMethodConfirm       = "confirm"
	UIMethodInput         = "input"
	UIMethodEditor        = "editor"
	UIMethodNotify        = "notify"
	UIMethodSetStatus     = "setStatus"
	UIMethodSetWidget     = "setWidget"
	UIMethodSetTitle      = "setTitle"
	UIMethodSetEditorText = "set_editor_text"
)

// BlockingUIMethod reports whether method is a dialog pi waits on (as opposed
// to a fire-and-forget notification).
func BlockingUIMethod(method string) bool {
	switch method {
	case UIMethodSelect, UIMethodConfirm, UIMethodInput, UIMethodEditor:
		return true
	}
	return false
}

// UIRequest is a decoded extension_ui_request. Only the fields of the request's
// own method are set; Raw always carries the complete frame.
type UIRequest struct {
	ID       string
	Method   string
	Blocking bool // true when pi waits for an answer

	// Dialog payloads.
	Title       string
	Message     string
	Options     []string
	Placeholder string
	Prefill     string
	TimeoutMS   int

	// Notification payloads.
	NotifyType      string // notify: "info", "warning" or "error"
	StatusKey       string // setStatus
	StatusText      string // setStatus
	WidgetKey       string // setWidget
	WidgetLines     []string
	WidgetPlacement string // setWidget: "aboveEditor" or "belowEditor"
	Text            string // set_editor_text

	Raw json.RawMessage
}

// UIRequest decodes an extension_ui_request event. It reports false for any
// other event type or a frame that does not decode.
func (e Event) UIRequest() (UIRequest, bool) {
	if e.Type != "extension_ui_request" {
		return UIRequest{}, false
	}
	var wire struct {
		ID              string   `json:"id"`
		Method          string   `json:"method"`
		Title           string   `json:"title"`
		Message         string   `json:"message"`
		Options         []string `json:"options"`
		Placeholder     string   `json:"placeholder"`
		Prefill         string   `json:"prefill"`
		Timeout         int      `json:"timeout"`
		NotifyType      string   `json:"notifyType"`
		StatusKey       string   `json:"statusKey"`
		StatusText      string   `json:"statusText"`
		WidgetKey       string   `json:"widgetKey"`
		WidgetLines     []string `json:"widgetLines"`
		WidgetPlacement string   `json:"widgetPlacement"`
		Text            string   `json:"text"`
	}
	if err := e.Unmarshal(&wire); err != nil {
		return UIRequest{}, false
	}
	return UIRequest{
		ID:              wire.ID,
		Method:          wire.Method,
		Blocking:        BlockingUIMethod(wire.Method),
		Title:           wire.Title,
		Message:         wire.Message,
		Options:         wire.Options,
		Placeholder:     wire.Placeholder,
		Prefill:         wire.Prefill,
		TimeoutMS:       wire.Timeout,
		NotifyType:      wire.NotifyType,
		StatusKey:       wire.StatusKey,
		StatusText:      wire.StatusText,
		WidgetKey:       wire.WidgetKey,
		WidgetLines:     wire.WidgetLines,
		WidgetPlacement: wire.WidgetPlacement,
		Text:            wire.Text,
		Raw:             e.Raw,
	}, true
}

// Value answers a select, input or editor request with the chosen text.
func (r UIRequest) Value(value string) map[string]any {
	return map[string]any{"value": value}
}

// Confirmed answers a confirm request.
func (r UIRequest) Confirmed(confirmed bool) map[string]any {
	return map[string]any{"confirmed": confirmed}
}

// Cancelled dismisses any dialog. The extension receives undefined (or false
// for confirm).
func (r UIRequest) Cancelled() map[string]any {
	return map[string]any{"cancelled": true}
}

// NotifyTypeOrInfo is the notification kind, defaulting to "info" as pi does.
func (r UIRequest) NotifyTypeOrInfo() string {
	if r.NotifyType == "" {
		return "info"
	}
	return r.NotifyType
}
