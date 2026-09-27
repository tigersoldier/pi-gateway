;;; gateway-cases.el --- gateway-specific end-to-end cases with the real client -*- lexical-binding: t; -*-

;;; Commentary:

;; The cases here test the gateway itself, with pilish's real RPC client as the
;; UI: session survival across a client drop, two clients on one session,
;; daemon restart re-adoption, hibernation, CLI surfaces, failure modes, and
;; restricted tokens.  Every case runs against a daemon started by run.sh;
;; lanes that do not apply are skipped rather than faked.

;;; Code:

(require 'cl-lib)
(require 'ert)
(require 'seq)

(load (expand-file-name "e2e-common.el"
                        (file-name-directory (or load-file-name buffer-file-name))))

(defun e2e-prompt-text (real-text)
  "Return REAL-TEXT for the real lane and a scenario-safe text otherwise."
  (if (eq e2e-lane 'real) real-text "e2e prompt"))

;;;; 1. Session survival across a client drop (the ssh-disconnect case)

(ert-deftest gateway-e2e-client-death-survives-turn ()
  "A dropped client must not end the turn or the session."
  (e2e-require-lane 'real 'fake)
  (let* ((daemon (e2e-lane-daemon))
         (a (e2e-start daemon))
         path)
    (unwind-protect
        (progn
          (setq path (e2e-session-file a))
          (e2e-prompt a (e2e-prompt-text "/no_think Count from 1 to 60 slowly"))
          ;; Drop the client while the turn is running, like an ssh hangup.
          (e2e-kill a)
          (ert-info ("the daemon keeps the session and its pi process")
            (e2e-wait-row path (lambda (row) (alist-get 'live row)) 15
                          "the dropped client's session stays live" daemon))
          (let ((b (e2e-start daemon)))
            (unwind-protect
                (progn
                  (e2e-switch b path)
                  (let ((state (e2e-wait-idle b 120)))
                    (ert-info ("the turn completed with no client attached")
                      (should state)
                      (should (>= (plist-get state :messageCount) 2))
                      (should (equal (plist-get state :sessionFile) path))))
                  ;; The session is still usable: a new turn goes through.
                  (e2e-prompt b (e2e-prompt-text "/no_think Say: after-death"))
                  (let ((state (e2e-wait-messages b 4 120)))
                    (should (>= (or (plist-get state :messageCount) 0) 4))))
              (e2e-stop b))))
      (e2e-stop a))))

;;;; 2. Two clients, one session

(ert-deftest gateway-e2e-two-clients-share-one-session ()
  "A second client attaches to a live session and both see later turns."
  (e2e-require-lane 'real 'fake)
  (let* ((daemon (e2e-lane-daemon))
         (a (e2e-start daemon))
         (b nil)
         path)
    (unwind-protect
        (progn
          (setq path (e2e-session-file a))
          (e2e-prompt a (e2e-prompt-text "/no_think Say: first"))
          (e2e-wait-messages a 2 120)
          (setq b (e2e-start daemon))
          (e2e-switch b path)
          (ert-info ("both clients share one live session")
            (should (equal (e2e-session-file b) path))
            (e2e-wait-row path
                          (lambda (row) (and (alist-get 'live row)
                                             (>= (length (alist-get 'clients row)) 2)))
                          15 "two clients on the same session" daemon))
          ;; A prompt from B streams to A too.
          (e2e-forget-events a)
          (e2e-prompt b (e2e-prompt-text "/no_think Say: from-b"))
          (e2e-wait-messages b 4 120)
          (ert-info ("the other client observes B's turn")
            (e2e-event-seen-p a "agent_settled" 120 "B's turn to settle for A")))
      (e2e-stop a)
      (e2e-stop b))))

;;;; 3. Daemon restart re-adoption

(ert-deftest gateway-e2e-daemon-restart-reattach ()
  "A restarted daemon re-adopts the session file and resumes its history."
  ;; The scenario fake has no --session resume, so this is real-pi only.
  (e2e-require-lane 'real)
  (let* ((daemon (e2e-lane-daemon))
         (a (e2e-start daemon))
         path count)
    (unwind-protect
        (progn
          (setq path (e2e-session-file a))
          (e2e-prompt a (e2e-prompt-text "/no_think Say: before-restart"))
          (setq count (plist-get (e2e-wait-messages a 2 120) :messageCount))
          (e2e-stop a)
          (setq a nil)
          (e2e-restart-daemon (alist-get 'name daemon))
          ;; The old daemon is gone: nothing is live anymore.
          (let ((b (e2e-start (e2e-lane-daemon))))
            (unwind-protect
                (progn
                  (e2e-switch b path)
                  (let ((state (e2e-wait-messages b count 60)))
                    (ert-info ("history is preserved across the restart")
                      (should (>= (or (plist-get state :messageCount) 0) count))))
                  (e2e-prompt b (e2e-prompt-text "/no_think Say: after-restart"))
                  (should (>= (or (plist-get (e2e-wait-messages b (+ count 2) 120)
                                             :messageCount)
                                  0)
                              (+ count 2))))
              (e2e-stop b))))
      (e2e-stop a))))

;;;; 4. Hibernation and respawn

(ert-deftest gateway-e2e-hibernation-respawns ()
  "An idle session is reaped and the next attach respawns pi with its history."
  (e2e-require-lane 'real)
  (let* ((daemon (e2e-daemon "hibernate"))
         (a (e2e-start daemon))
         path count)
    (unwind-protect
        (progn
          (setq path (e2e-session-file a))
          (e2e-prompt a (e2e-prompt-text "/no_think Say: before-hibernate"))
          (setq count (plist-get (e2e-wait-messages a 2 120) :messageCount))
          (e2e-stop a)
          (setq a nil)
          (ert-info ("idle timeout stops pi")
            (should (= 0 (e2e-wait-live 0 30 daemon))))
          (let ((b (e2e-start daemon)))
            (unwind-protect
                (progn
                  (e2e-switch b path)
                  (let ((state (e2e-wait-messages b count 60)))
                    (ert-info ("the respawned session keeps its history")
                      (should (>= (or (plist-get state :messageCount) 0) count))))
                  (e2e-prompt b (e2e-prompt-text "/no_think Say: after-hibernate"))
                  (should (>= (or (plist-get (e2e-wait-messages b (+ count 2) 120)
                                             :messageCount)
                                  0)
                              (+ count 2))))
              (e2e-stop b))))
      (e2e-stop a))))

;;;; 5. CLI surfaces

(ert-deftest gateway-e2e-cli-version-help-and-mode ()
  "`--version' is answered by the daemon; `--help' is client-local."
  (e2e-require-lane 'real 'fake)
  (let* ((daemon (e2e-lane-daemon))
         (version (alist-get 'piVersion (e2e-status daemon))))
    (ert-info ("--version reports the managed pi version")
      (let* ((res (e2e-call (list "--version"
                                  "--server" (alist-get 'addr daemon)
                                  "--token-file" (alist-get 'token daemon))))
             (exit (nth 0 res))
             (out (string-trim (nth 1 res))))
        (should (= 0 exit))
        (should (equal out version))))
    (ert-info ("--help works without a daemon")
      (let* ((res (e2e-call (list "--help" "--server" "127.0.0.1:1")))
             (exit (nth 0 res))
             (out (nth 1 res)))
        (should (= 0 exit))
        (should (string-match-p "Usage:" out))
        (should (string-match-p "--mode rpc" out))))
    (ert-info ("only --mode rpc is served")
      (let* ((res (e2e-call (list "--mode" "tui")))
             (exit (nth 0 res))
             (out (nth 1 res)))
        (should (/= 0 exit))
        (should (string-match-p "only --mode rpc" out))))))

;;;; 6. Failure modes

(ert-deftest gateway-e2e-client-failure-modes ()
  "Missing daemon, bad token and unreadable token fail fast and loudly."
  (e2e-require-lane 'real 'fake)
  (let* ((daemon (e2e-lane-daemon))
         (closed (format "127.0.0.1:%d" (e2e-free-port)))
         (bad-token (make-temp-file "e2e-bad-token" nil "not-the-token\n")))
    (ert-info ("daemon down: clear error, no hang")
      (let* ((res (e2e-call (list "--mode" "rpc" "--server" closed
                                  "--token-file" (alist-get 'token daemon))
                            10))
             (exit (nth 0 res))
             (out (nth 1 res)))
        (should (/= 0 exit))
        (should (string-match-p "connect\\|refused\\|daemon" out))))
    (ert-info ("bad token is rejected by the daemon")
      (let* ((res (e2e-call (list "--mode" "rpc"
                                  "--server" (alist-get 'addr daemon)
                                  "--token-file" bad-token)
                            15))
             (exit (nth 0 res))
             (out (nth 1 res)))
        (should (/= 0 exit))
        (should (string-match-p "unauthorized\\|token" out))))
    (ert-info ("unreadable token file is reported")
      (let* ((res (e2e-call (list "--mode" "rpc"
                                  "--server" (alist-get 'addr daemon)
                                  "--token-file" "/nonexistent/token")))
             (exit (nth 0 res))
             (out (nth 1 res)))
        (should (/= 0 exit))
        (should (string-match-p "cannot read token" out))))
    (delete-file bad-token)))

;;;; 7. Restricted tokens end to end

(ert-deftest gateway-e2e-restricted-token-roles ()
  "An observer token may read but not prompt, mutate, or run bash."
  (e2e-require-lane 'real 'fake)
  (let* ((daemon (e2e-lane-daemon))
         (observer (alist-get 'observerToken daemon))
         (registered-before (alist-get 'registered (alist-get 'sessions (e2e-status daemon)))))
    (unless observer
      (ert-skip "no observer token provisioned for this run"))
    (let ((session (e2e-start-with-token daemon observer)))
      (unwind-protect
          (progn
            (ert-info ("observe still allows reads")
              (let ((state (e2e-state session)))
                (should (plist-get state :sessionFile))))
            (ert-info ("prompt is refused with forbidden")
              (let ((resp (e2e-rpc session '(:type "prompt" :message "nope") 30)))
                (should resp)
                (should (eq (plist-get resp :success) :false))
                (should (equal (plist-get resp :code) "forbidden"))))
            (ert-info ("bash is refused with forbidden")
              (let ((resp (e2e-rpc session '(:type "bash" :command "true") 30)))
                (should resp)
                (should (eq (plist-get resp :success) :false))
                (should (equal (plist-get resp :code) "forbidden"))))
            (ert-info ("shared-state mutations are refused")
              (let ((resp (e2e-rpc session '(:type "set_model" :modelId "x") 30)))
                (should resp)
                (should (eq (plist-get resp :success) :false))
                (should (equal (plist-get resp :code) "forbidden")))))
        (e2e-stop session)))))

;;;; 8. Spawn parameters and conflicts

(ert-deftest gateway-e2e-spawn-param-conflict ()
  "A differing value for a recorded spawn key is refused; a key the live
session never recorded is ignored and still attaches (docs/protocol.md 4.3)."
  (e2e-require-lane 'real 'fake)
  (let* ((daemon (e2e-lane-daemon))
         (a (e2e-start daemon (list "--session-dir" (make-temp-file "e2e-a" t))))
         (b (e2e-start daemon (list "--session-dir" (make-temp-file "e2e-b" t))))
         (c (e2e-start daemon '("--approve")))
         path)
    (unwind-protect
        (progn
          (setq path (e2e-session-file a))
          (ert-info ("a different value for the recorded key is refused")
            (let ((resp (e2e-rpc b (list :type "switch_session" :sessionPath path) 30)))
              (should resp)
              (should (eq (plist-get resp :success) :false))
              (should (equal (plist-get resp :code) "spawn_param_conflict"))))
          (ert-info ("an unrecorded spawn key does not refuse attachment")
            (e2e-switch c path)
            (should (equal (e2e-session-file c) path))))
      (e2e-stop a)
      (e2e-stop b)
      (e2e-stop c))))

;;;; 9. The session's working directory

(ert-deftest gateway-e2e-session-follows-client-directory ()
  "A new session runs pi in the client's directory, not the daemon's."
  (e2e-require-lane 'real 'fake)
  (let* ((daemon (e2e-lane-daemon))
         (project (make-temp-file "e2e-project" t))
         (a (e2e-start daemon nil project))
         path)
    (unwind-protect
        (progn
          (setq path (e2e-session-file a))
          ;; pi creates the session file with the first message, so run one
          ;; short turn before reading the header.
          (e2e-prompt a (e2e-prompt-text "/no_think Say: cwd"))
          (e2e-wait-messages a 2 120)
          (e2e-wait-file path 30)
          (ert-info ("the session records the client's directory")
            (should (equal (e2e-session-cwd path)
                           (file-truename (directory-file-name project)))))
          (ert-info ("which is not the daemon's directory")
            (should-not (equal (e2e-session-cwd path)
                               (file-truename (directory-file-name e2e-work-dir))))))
      (e2e-stop a))))

(message "e2e: gateway cases lane=%s gateway=%s" e2e-lane e2e-gateway-bin)

(ert-run-tests-batch-and-exit (or (getenv "E2E_SELECTOR") "gateway-e2e-"))


(provide 'gateway-cases)
;;; gateway-cases.el ends here
