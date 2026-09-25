;;; pilish-suites.el --- pilish's integration contracts through pi-gateway -*- lexical-binding: t; -*-

;;; Commentary:

;; Runs pilish's own integration suite with `pilish-executable' re-pointed at
;; the pi-gateway bridge, so every contract travels the real path:
;;
;;   Emacs (pilish) -> pi-gateway -> TCP -> pi-gatewayd -> pi
;;
;; The suite's two backends are kept, but both now mean "through the gateway":
;;
;;   fake  the daemon spawns pilish's scenario-driven fake pi; the scenario is
;;         baked into a wrapper by run.sh and selected per test via the daemon
;;         registry (a client cannot pass --scenario: piargs rejects unknown
;;         pi options, by design).
;;   real  the daemon spawns the real pi binary with the configured model.
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
         (daemon (if (eq backend 'real)
                     (e2e-daemon "real")
                   (e2e-daemon (format "%s" scenario)))))
    (if (and daemon
             (pilish-integration--backend-enabled-p backend))
        (progn
          (setq spec (plist-put spec :executable (list e2e-gateway-bin)))
          (plist-put spec :extra-args (e2e-server-args daemon)))
      spec)))

(advice-add 'pilish-test-backend-spec :filter-return #'e2e-pilish-spec)

(message "e2e: lane=%s gateway=%s daemons=%s"
         e2e-lane e2e-gateway-bin e2e-daemons-file)

(ert-run-tests-batch-and-exit
 (or (getenv "E2E_SELECTOR") "pilish-integration-.*/\\(fake\\|real\\)"))

;;; pilish-suites.el ends here
