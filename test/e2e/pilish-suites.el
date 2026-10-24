;;; pilish-suites.el --- pilish's integration contracts through pi-gateway -*- lexical-binding: t; -*-

;;; Commentary:

;; Runs pilish's own integration suite with `pilish-executable' re-pointed at
;; the pi-gateway bridge, so every contract travels the real path:
;;
;;   Emacs (pilish) -> pi-gateway -> TCP -> pi-gatewayd -> pi
;;
;; The shared suite keeps its fake and real test variants. In the TypeScript
;; local lane, the fake variant is the deterministic contract selector, but
;; the daemon uses the real Pi SDK and the local scripted provider; no fake Pi
;; subprocess or --scenario option is involved. Go lanes retain their original
;; scenario-wrapper and configured-model mappings.
;;
;; The daemon-selection flags are injected through
;; `pilish-test-backend-spec' so nothing in the pilish checkout changes.

;;; Code:

(setq load-prefer-newer t)
(require 'package)
(let ((dir (getenv "PACKAGE_USER_DIR")))
  (when dir
    (setq package-user-dir (directory-file-name (expand-file-name dir)))))
(package-initialize)

(let* ((pilish-dir (or (getenv "E2E_PILISH_DIR")
                       (error "E2E_PILISH_DIR is not set")))
       (test-dir (expand-file-name "test" pilish-dir)))
  (setq load-path (cons pilish-dir load-path))
  (setq load-path (cons test-dir load-path)))

(require 'pilish)
(require 'pilish-test-common)
(require 'pilish-integration-test-common)
(require 'pilish-integration-test)          ; all shared contract suites

(load (expand-file-name "e2e-common.el"
                        (file-name-directory (or load-file-name buffer-file-name))))

(defun e2e-pilish-spec (spec)
  "Point SPEC's executable at the gateway and select its daemon.
Backends that this run does not enable are returned untouched so the suite's
own skip logic reports them."
  (let* ((backend (plist-get spec :name))
         (scenario (or (plist-get spec :scenario)
                       pilish-integration--default-fake-scenario))
         (daemon (cond
                  ((eq e2e-lane 'local) (e2e-daemon "local"))
                  ((eq backend 'real) (e2e-daemon "real"))
                  (t (e2e-daemon (format "%s" scenario))))))
    (if (and daemon
             (pilish-integration--backend-enabled-p backend))
        (progn
          (setq spec (plist-put spec :executable (list e2e-gateway-bin)))
          (plist-put spec :extra-args (e2e-server-args daemon)))
      spec)))

(advice-add 'pilish-test-backend-spec :filter-return #'e2e-pilish-spec)

(defun e2e-typescript-local-text-delta-p (events)
  "Return non-nil when EVENTS include a streamed assistant text delta."
  (seq-find (lambda (event)
              (and (equal (plist-get event :type) "message_update")
                   (equal (plist-get (plist-get event :assistantMessageEvent) :type)
                          "text_delta")))
            events))

(defun e2e-typescript-local-user-message-p (text)
  "Return an event predicate matching user messages containing TEXT."
  (lambda (events)
    (seq-find (lambda (event)
                (and (equal (plist-get event :type) "message_start")
                     (equal (plist-get (plist-get event :message) :role) "user")
                     (string-match-p text
                                     (pilish-integration--message-text
                                      (plist-get event :message)))))
              events)))

(defun e2e-typescript-local-abort-and-wait (session description)
  "Require the abort RPC and settlement within 10 seconds total."
  (let* ((deadline (+ (float-time) 10.0))
         (abort-timeout (- deadline (float-time))))
    (unless (> abort-timeout 0)
      (error "abort deadline expired"))
    (e2e-rpc! session '(:type "abort") abort-timeout)
    (let ((settle-timeout (- deadline (float-time))))
      (unless (> settle-timeout 0)
        (error "settlement deadline expired: %s" description))
      (e2e-event-seen-p session "agent_settled" settle-timeout description))))

(ert-deftest typescript-local-contract-abort-after-streaming-delta ()
  "Abort an actual local-provider stream after Pilish observes its first delta."
  (unless (eq e2e-lane 'local) (ert-skip "TypeScript local lane only"))
  (let* ((session (e2e-start (e2e-daemon "local")))
         (proc (plist-get session :proc)))
    (unwind-protect
        (progn
          (e2e-prompt session "typescript-local-contract-abort")
          (e2e-wait-for session #'e2e-typescript-local-text-delta-p 15
                        "a streamed provider delta before abort")
          (e2e-typescript-local-abort-and-wait session "settlement after streamed abort")
          (should (eq (plist-get (e2e-state session) :isStreaming) :false)))
      (when (process-live-p proc) (e2e-stop session)))))

(ert-deftest typescript-local-contract-steer-during-streaming ()
  "Deliver steering while the local SDK has an observed provider stream in flight."
  (unless (eq e2e-lane 'local) (ert-skip "TypeScript local lane only"))
  (let* ((session (e2e-start (e2e-daemon "local")))
         (proc (plist-get session :proc)))
    (unwind-protect
        (progn
          (e2e-prompt session "typescript-local-contract-steer-initial")
          (e2e-wait-for session #'e2e-typescript-local-text-delta-p 15
                        "a streamed provider delta before steer")
          (e2e-rpc! session '(:type "steer" :message "queued-steer-test"))
          (e2e-wait-for session (e2e-typescript-local-user-message-p "queued-steer-test") 15
                        "delivery of the queued user message")
          (e2e-typescript-local-abort-and-wait session "settlement after steered-turn abort")
          (let ((user-messages
                 (seq-filter (lambda (event)
                              (and (equal (plist-get event :type) "message_start")
                                   (equal (plist-get (plist-get event :message) :role) "user")))
                            (e2e-events session))))
            (should (= (length user-messages) 2)))
          (should (eq (plist-get (e2e-state session) :isStreaming) :false)))
      (when (process-live-p proc) (e2e-stop session)))))

(message "e2e: lane=%s gateway=%s daemons=%s"
         e2e-lane e2e-gateway-bin e2e-daemons-file)

(ert-run-tests-batch-and-exit
 (or (let ((selector (getenv "E2E_SELECTOR")))
       (and selector (not (equal selector "")) selector))
     (if (eq e2e-lane 'local)
         "\\(pilish-integration-.*/fake\\|typescript-local-contract-\\)"
       "pilish-integration-.*/\\(fake\\|real\\)")))

;;; pilish-suites.el ends here
