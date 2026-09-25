;;; e2e-common.el --- shared helpers for pi-gateway end-to-end tests -*- lexical-binding: t; -*-

;;; Commentary:

;; Helpers shared by the two end-to-end suites under test/e2e.  Everything
;; here talks to a real pi-gatewayd over TCP through the pi-gateway bridge and
;; drives the real pilish client code; nothing stubs the gateway.
;;
;; The runner (test/e2e/run.sh) starts the daemons, writes daemons.json and
;; exports E2E_DAEMONS_FILE / E2E_WORK_DIR / E2E_GATEWAY_BIN / E2E_LANE.

;;; Code:

(require 'cl-lib)
(require 'ert)
(require 'json)
(require 'seq)
(require 'url)

(setq json-array-type 'list)

;; The suites drive pilish's real client code, so the checkout (and the
;; package dir the runner installed) must be in place before anything else.
(setq load-prefer-newer t)
(require 'package)
(let ((dir (getenv "PACKAGE_USER_DIR")))
  (when dir
    (setq package-user-dir (directory-file-name (expand-file-name dir)))))
(package-initialize)
(let ((pilish-dir (or (getenv "E2E_PILISH_DIR")
                      (error "E2E_PILISH_DIR is not set"))))
  (setq load-path (cons pilish-dir load-path)))
(require 'pilish)

(defvar e2e-lane (intern (or (getenv "E2E_LANE") "real"))
  "Lane under test: `real' (real pi) or `fake' (scenario-driven fake pi).")

(defvar e2e-gateway-bin (getenv "E2E_GATEWAY_BIN")
  "Path to the pi-gateway bridge binary under test.")

(defvar e2e-daemons-file (getenv "E2E_DAEMONS_FILE")
  "Path to the daemon registry written by run.sh.")

(defvar e2e-work-dir (or (getenv "E2E_WORK_DIR") default-directory)
  "Working directory the test daemons were started in.")

(defvar e2e-restart-script (getenv "E2E_RESTART_SCRIPT")
  "Script from run.sh that stops and restarts a named daemon.")

(defun e2e-daemons ()
  "Return the daemon registry as an alist keyed by symbol name."
  (with-temp-buffer
    (insert-file-contents e2e-daemons-file)
    (json-read-from-string (buffer-string))))

(defun e2e-daemon (name)
  "Return the daemon record for NAME (a string) or nil."
  (alist-get (intern name) (e2e-daemons)))

(defvar e2e-lane-daemons '((real . "real") (fake . "prompt-lifecycle"))
  "Daemon registry entry that serves each lane in the gateway cases.")

(defun e2e-lane-daemon ()
  "Return the daemon record serving the selected lane."
  (or (e2e-daemon (or (alist-get e2e-lane e2e-lane-daemons)
                      (symbol-name e2e-lane)))
      (error "no daemon registered for lane %s" e2e-lane)))

(defun e2e-require-lane (&rest lanes)
  "Skip the current test unless the lane is one of LANES."
  (unless (memq e2e-lane lanes)
    (ert-skip (format "lane %s not selected (want %s)"
                      e2e-lane (mapconcat #'symbol-name lanes ", ")))))

(defun e2e-server-args (daemon &optional token-file)
  "Return client arguments that point DAEMON at its daemon.
TOKEN-FILE overrides the daemon's default token."
  (list "--server" (alist-get 'addr daemon)
        "--token-file" (or token-file (alist-get 'token daemon))))

;;;; Gateway client sessions (pilish drives the bridge)

(defun e2e-start (daemon &optional extra-args directory token-file)
  "Start a gateway-backed pilish process against DAEMON.
EXTRA-ARGS are appended after the daemon-selection options (so a test can add
pi options such as `--model').  DIRECTORY defaults to `e2e-work-dir'.
TOKEN-FILE overrides the daemon's default token.  Returns a session plist
accepted by the other helpers here."
  (let* ((default-directory (or directory e2e-work-dir))
         (pilish-executable (list e2e-gateway-bin))
         (pilish-extra-args (append (e2e-server-args daemon token-file)
                                    extra-args))
         (events (list nil))
         (proc (pilish--start-process default-directory)))
    (process-put proc 'pilish-display-handler
                 (lambda (event) (setcar events (cons event (car events)))))
    (list :proc proc :events events :daemon daemon)))

(defun e2e-stop (session)
  "Stop SESSION's client process, if it is still alive."
  (when-let* ((proc (plist-get session :proc)))
    (when (process-live-p proc)
      (delete-process proc))))

(defun e2e-kill (session)
  "Drop SESSION's client abruptly, the way an ssh disconnect would."
  (e2e-stop session))

(defun e2e-events (session)
  "Return SESSION's received events, oldest first."
  (reverse (car (plist-get session :events))))

(defun e2e-forget-events (session)
  "Discard SESSION's buffered events."
  (setcar (plist-get session :events) nil))

(defun e2e-rpc (session request &optional timeout)
  "Send REQUEST through SESSION and return the response plist or nil."
  (pilish--rpc-sync (plist-get session :proc) request (or timeout 30)))

(defun e2e-rpc! (session request &optional timeout)
  "Like `e2e-rpc' but signal unless the response reports success."
  (let ((response (e2e-rpc session request timeout)))
    (unless response
      (error "no response to %S (client died?)" (plist-get request :type)))
    (unless (eq (plist-get response :success) t)
      (error "%S failed: %S" (plist-get request :type) response))
    response))

(defun e2e-state (session &optional timeout)
  "Return the `:data' plist of a fresh get_state from SESSION."
  (plist-get (e2e-rpc! session '(:type "get_state") timeout) :data))

(defun e2e-wait-file (path timeout)
  "Wait until PATH exists and has content, then return it.
pi reports its session file as soon as it starts but writes the header a
moment later, so reading a fresh session's header needs this wait."
  (let ((deadline (+ (float-time) timeout)))
    (while (and (not (and (file-exists-p path)
                          (file-attribute-size (file-attributes path))))
                (< (float-time) deadline))
      (sleep-for 0.1))
    (unless (file-exists-p path)
      (error "session file %s never appeared" path))
    path))

(defun e2e-session-cwd (path)
  "Return the working directory recorded in PATH's session header."
  (with-temp-buffer
    (insert-file-contents path nil 0 4096)
    (goto-char (point-min))
    (let ((line (buffer-substring-no-properties (point) (line-end-position))))
      (alist-get 'cwd (json-read-from-string line)))))

(defun e2e-session-file (session &optional timeout)
  "Return SESSION's session file path."
  (or (plist-get (e2e-state session timeout) :sessionFile)
      (error "get_state returned no sessionFile")))

(defun e2e-switch (session path &optional timeout)
  "Attach SESSION to the session file at PATH."
  (e2e-rpc! session (list :type "switch_session" :sessionPath path) timeout))

(defun e2e-prompt (session message &optional timeout)
  "Send MESSAGE as a prompt through SESSION."
  (e2e-rpc! session (list :type "prompt" :message message) (or timeout 60)))

(defun e2e-wait-for (session predicate timeout description)
  "Wait until PREDICATE accepts SESSION's events, or fail after TIMEOUT.
PREDICATE receives the event list oldest-first; DESCRIPTION appears in the
failure message."
  (let ((deadline (+ (float-time) timeout)))
    (while (and (not (funcall predicate (e2e-events session)))
                (< (float-time) deadline))
      (accept-process-output (plist-get session :proc) 0.2))
    (or (funcall predicate (e2e-events session))
        (error "timeout after %ss waiting for %s; got %S"
               timeout description
               (mapcar (lambda (event) (plist-get event :type))
                       (e2e-events session))))))

(defun e2e-event-seen-p (session type timeout description)
  "Wait for an event of TYPE in SESSION."
  (e2e-wait-for session
                (lambda (events)
                  (seq-find (lambda (event) (equal (plist-get event :type) type))
                            events))
                timeout description))

(defun e2e-wait-idle (session timeout)
  "Wait until SESSION reports isStreaming false and return its state."
  (let ((deadline (+ (float-time) timeout))
        state)
    (while (and (progn (setq state (e2e-state session))
                       (not (eq (plist-get state :isStreaming) :false)))
                (< (float-time) deadline))
      (sleep-for 0.3))
    (when (eq (plist-get state :isStreaming) :false)
      state)))

(defun e2e-wait-messages (session count timeout)
  "Wait until SESSION reports at least COUNT messages and return its state."
  (let ((deadline (+ (float-time) timeout))
        state)
    (while (and (progn (setq state (e2e-state session))
                       (< (or (plist-get state :messageCount) 0) count))
                (< (float-time) deadline))
      (sleep-for 0.3))
    state))

;;;; Daemon-side inspection (read-only debug listener)

(defun e2e-debug-json (path &optional daemon)
  "Fetch PATH from DAEMON's debug listener and return it parsed."
  (let* ((daemon (or daemon (e2e-lane-daemon)))
         (addr (alist-get 'debug daemon)))
    (unless addr (error "daemon %S has no debug listener" (alist-get 'name daemon)))
    (with-temp-buffer
      (url-insert-file-contents (format "http://%s%s" addr path))
      (json-read))))

(defun e2e-status (&optional daemon)
  "Return the daemon /status document."
  (e2e-debug-json "/status" daemon))

(defun e2e-catalog-row (path &optional daemon)
  "Return the /catalog row for session PATH, or nil."
  (let ((rows (alist-get 'sessions (e2e-debug-json "/catalog" daemon))))
    (seq-find (lambda (row) (equal (alist-get 'path row) path)) rows)))

(defun e2e-wait-row (path predicate timeout description &optional daemon)
  "Wait until PATH's catalog row satisfies PREDICATE."
  (let ((deadline (+ (float-time) timeout))
        row)
    (while (and (progn (setq row (e2e-catalog-row path daemon))
                       (not (and row (funcall predicate row))))
                (< (float-time) deadline))
      (sleep-for 0.2))
    (unless (and row (funcall predicate row))
      (error "timeout after %ss waiting for %s (row %S)" timeout description row))
    row))

(defun e2e-live-sessions (&optional daemon)
  "Return the number of sessions with a live pi process."
  (let* ((status (e2e-status daemon))
         (sessions (alist-get 'sessions status)))
    (alist-get 'live sessions)))

(defun e2e-client-count (&optional daemon)
  "Return the number of attached clients the daemon reports."
  (alist-get 'clients (alist-get 'sessions (e2e-status daemon))))

(defun e2e-wait-live (expected timeout &optional daemon)
  "Wait until the daemon reports EXPECTED live sessions."
  (let ((deadline (+ (float-time) timeout))
        live)
    (while (and (progn (setq live (e2e-live-sessions daemon))
                       (/= live expected))
                (< (float-time) deadline))
      (sleep-for 0.2))
    live))

(defun e2e-start-with-token (daemon token-file &optional extra-args)
  "Start a client against DAEMON authenticating with TOKEN-FILE."
  (e2e-start daemon extra-args nil token-file))

;;;; Daemon lifecycle and ports

(defun e2e-free-port ()
  "Return a currently free loopback TCP port."
  (let* ((proc (make-network-process :name "e2e-port-probe" :server t
                                     :host "127.0.0.1" :service t :noquery t))
         (contact (process-contact proc))
         (port (if (listp contact) (car (last contact)) contact)))
    (delete-process proc)
    (or port (error "could not allocate a port"))))

(defun e2e-restart-daemon (name)
  "Restart the daemon called NAME through run.sh and wait for its port."
  (unless (and e2e-restart-script (file-executable-p e2e-restart-script))
    (error "E2E_RESTART_SCRIPT is not set"))
  (let ((buffer (generate-new-buffer " *e2e-restart*")))
    (unwind-protect
        (progn
          (unless (zerop (call-process "bash" nil buffer nil e2e-restart-script name))
            (error "restart of %s failed: %s" name
                   (with-current-buffer buffer (buffer-string))))
          (let ((deadline (+ (float-time) 20)))
            (while (and (not (e2e-port-open-p (alist-get 'addr (e2e-daemon name))))
                        (< (float-time) deadline))
              (sleep-for 0.2))
            (unless (e2e-port-open-p (alist-get 'addr (e2e-daemon name)))
              (error "restarted daemon %s never accepted connections" name))))
      (kill-buffer buffer))))

(defun e2e-port-open-p (addr)
  "Return non-nil when ADDR (host:port) accepts a TCP connection."
  (let* ((parts (split-string addr ":"))
         (host (nth 0 parts))
         (port (string-to-number (nth 1 parts))))
    (condition-case nil
        (let ((proc (make-network-process :name "e2e-probe" :host host
                                          :service port :noquery t)))
          (delete-process proc)
          t)
      (error nil))))

;;;; CLI invocation (no pilish involved)

(defun e2e-call (args &optional timeout)
  "Run the gateway bridge with ARGS and return (EXIT STDOUT+STDERR).
TIMEOUT defaults to 15s; coreutils `timeout' kills a hung client, which
reports exit 124.  Output goes to dedicated buffers so that Emacs process
sentinels cannot leak into it."
  ;; call-process wants (STDOUT-DESTINATION STDERR-FILE): a buffer for stdout,
  ;; a file name for stderr.
  (let* ((out (generate-new-buffer " *e2e-out*"))
         (err (make-temp-file "e2e-err"))
         (exit (apply #'call-process "timeout" nil (list out err) nil
                      (number-to-string (or timeout 15)) e2e-gateway-bin args))
         (text (concat (with-current-buffer out (buffer-string))
                       (with-temp-buffer (insert-file-contents err) (buffer-string)))))
    (kill-buffer out)
    (delete-file err)
    (list exit text)))

(provide 'e2e-common)
;;; e2e-common.el ends here
