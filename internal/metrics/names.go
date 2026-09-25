package metrics

// Metric names exposed on the debug listener's /metrics endpoint. Counters end
// in _total, gauges do not. Names are part of the operational surface; keep
// them stable.
const (
	SessionsStarted = "pi_gateway_sessions_started_total"
	SessionsEnded   = "pi_gateway_sessions_ended_total"
	SessionsReaped  = "pi_gateway_sessions_reaped_total"
	Attaches        = "pi_gateway_attaches_total"
	AttachFailures  = "pi_gateway_attach_failures_total"
	Unauthorized    = "pi_gateway_unauthorized_total"
	TurnsStarted    = "pi_gateway_turns_started_total"
	TurnsSettled    = "pi_gateway_turns_settled_total"
	PromptsQueued   = "pi_gateway_prompts_queued_total"
	PromptsRejected = "pi_gateway_prompts_rejected_total"
	Reloads         = "pi_gateway_reloads_total"
	ReloadsRefused  = "pi_gateway_reloads_refused_total"
	PiExits         = "pi_gateway_pi_exits_total"

	UIRequests   = "pi_gateway_ui_requests_total"
	UIAnswered   = "pi_gateway_ui_answered_total"
	UIStale      = "pi_gateway_ui_stale_total"
	UIUnroutable = "pi_gateway_ui_unroutable_total"

	SubscriberDrops = "pi_gateway_subscriber_drops_total"
	ClientLags      = "pi_gateway_client_lags_total"
	FramesIn        = "pi_gateway_frames_in_total"
	FramesOut       = "pi_gateway_frames_out_total"
	BytesIn         = "pi_gateway_bytes_in_total"
	BytesOut        = "pi_gateway_bytes_out_total"
	FramesDropped   = "pi_gateway_frames_dropped_total"
)

// Gauge names exposed on /metrics; gauge values are read at scrape time.
const (
	GaugeSessions          = "pi_gateway_sessions"
	GaugeSessionsLive      = "pi_gateway_sessions_live"
	GaugeSessionsStreaming = "pi_gateway_sessions_streaming"
	GaugeClients           = "pi_gateway_clients"
	GaugeConnections       = "pi_gateway_connections"
	GaugeQueueDepth        = "pi_gateway_queue_depth"
	GaugeSubscribers       = "pi_gateway_subscribers"
	GaugeTokens            = "pi_gateway_tokens"
	GaugeUptime            = "pi_gateway_uptime_seconds"
)

// Help returns the HELP text for a declared metric.
func Help(name string) string { return helpText[name] }

var helpText = map[string]string{
	SessionsStarted: "sessions for which a pi process was started",
	SessionsEnded:   "sessions whose pi process exited or was stopped",
	SessionsReaped:  "sessions retired by the idle reaper",
	Attaches:        "successful client attachments to a session",
	AttachFailures:  "client attachments refused or timed out",
	Unauthorized:    "connections rejected for a missing or wrong token",
	TurnsStarted:    "agent turns started",
	TurnsSettled:    "agent turns that settled",
	PromptsQueued:   "prompts queued behind a running turn",
	PromptsRejected: "prompts rejected because the session was not running",
	Reloads:         "gw_reload_session restarts performed",
	ReloadsRefused:  "gw_reload_session requests refused as busy",
	PiExits:         "managed pi processes that exited",

	UIRequests:   "extension UI dialogs routed to a client",
	UIAnswered:   "extension UI dialogs answered by their owner",
	UIStale:      "extension UI answers from a client that no longer owns the dialog",
	UIUnroutable: "extension UI dialogs dropped because no ui-capable client was attached",

	SubscriberDrops: "subscribers dropped for not keeping up",
	ClientLags:      "gw_lag markers telling a lossy client which records it missed",
	FramesIn:        "protocol frames read from clients",
	FramesOut:       "protocol frames written to clients",
	BytesIn:         "protocol bytes read from clients",
	BytesOut:        "protocol bytes written to clients",
	FramesDropped:   "non-terminal frames dropped for lossy clients",

	GaugeSessions:          "registered sessions",
	GaugeSessionsLive:      "sessions with a running pi process",
	GaugeSessionsStreaming: "sessions currently running a turn",
	GaugeClients:           "clients attached to a session",
	GaugeConnections:       "open client connections",
	GaugeQueueDepth:        "prompts waiting in daemon queues",
	GaugeSubscribers:       "event subscribers",
	GaugeTokens:            "configured tokens",
	GaugeUptime:            "daemon uptime in seconds",
}

// counterNames lists the counters Declare registers, so the exposition is
// complete (and the HELP lines present) before the first increment.
var counterNames = []string{
	SessionsStarted, SessionsEnded, SessionsReaped,
	Attaches, AttachFailures, Unauthorized,
	TurnsStarted, TurnsSettled, PromptsQueued, PromptsRejected,
	Reloads, ReloadsRefused, PiExits,
	UIRequests, UIAnswered, UIStale, UIUnroutable,
	SubscriberDrops, ClientLags,
	FramesIn, FramesOut, BytesIn, BytesOut, FramesDropped,
}

// Declare registers every documented counter with its HELP text.
func (r *Registry) Declare() {
	if r == nil {
		return
	}
	for _, name := range counterNames {
		r.Counter(name, helpText[name])
	}
}
