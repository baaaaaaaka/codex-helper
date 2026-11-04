package migrate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// Workflow (Power Automate) card configuration, SPEC §5.2: the config is
// imported into meta and the webhook URL secret is copied next to the new
// store with mode 0600. The secret is never logged or put in the report.
const (
	workflowConfigName = "workflow-notifications.json"
	workflowSecretName = "workflow-webhook-url"

	MetaWorkflowEnabled    = "workflow_enabled"
	MetaWorkflowControlID  = "workflow_control_chat_id"
	MetaWorkflowSecretFile = "workflow_webhook_url_file"
)

type legacyWorkflow struct {
	Enabled               lbool  `json:"enabled"`
	ControlWebhookURLFile string `json:"control_webhook_url_file"`
	ControlChatID         string `json:"control_chat_id"`
}

func (c *converter) convertWorkflow() error {
	e := c.rep.E("workflow config")
	var cfg legacyWorkflow
	found := false
	// The sidecar file is authoritative; the store's copy is the fallback.
	if raw, err := os.ReadFile(filepath.Join(c.l.dir, workflowConfigName)); err == nil {
		var f struct {
			Workflow legacyWorkflow `json:"workflow"`
		}
		if _, err := decode(raw, &f); err != nil {
			c.rep.note("%s unreadable: %v", workflowConfigName, err)
		} else {
			cfg, found = f.Workflow, true
			e.Outcomes["from "+workflowConfigName]++
		}
	}
	if !found {
		if raw := c.l.section("workflow"); len(raw) > 0 && string(raw) != "null" {
			if _, err := decode(raw, &cfg); err == nil {
				found = true
				e.Outcomes["from the store"]++
			}
		}
	}
	if !found {
		e.Outcomes["absent"]++
		return nil
	}
	e.Source, e.Imported = 1, 1
	kv := map[string]string{MetaWorkflowEnabled: strconv.FormatBool(bool(cfg.Enabled))}
	if cfg.ControlChatID != "" {
		kv[MetaWorkflowControlID] = cfg.ControlChatID
	}
	secret, err := c.copyWorkflowSecret(cfg.ControlWebhookURLFile)
	switch {
	case err != nil:
		e.Outcomes["webhook secret not copied"]++
		c.rep.note("workflow webhook secret not copied: %v; cards stay off until `cxp teams workflow enable` is run again", err)
		kv[MetaWorkflowEnabled] = "false"
	case secret != "":
		kv[MetaWorkflowSecretFile] = secret
		e.Outcomes["webhook secret copied (0600)"]++
	}
	for _, k := range sortedKeys(kv) {
		if err := c.im.SetMeta(k, kv[k]); err != nil {
			return err
		}
	}
	return nil
}

// copyWorkflowSecret copies the webhook URL file into the output directory
// with mode 0600 and returns its new path. The source directory's own file
// is preferred over the configured path, which may point at an older layout.
func (c *converter) copyWorkflowSecret(configured string) (string, error) {
	candidates := []string{filepath.Join(c.l.dir, workflowSecretName)}
	if configured != "" {
		candidates = append(candidates, configured)
	}
	var src string
	for _, p := range candidates {
		if st, err := os.Stat(p); err == nil && st.Mode().IsRegular() {
			src = p
			break
		}
	}
	if src == "" {
		if configured == "" {
			return "", nil
		}
		return "", fmt.Errorf("no secret file (looked at %s and the configured path)", workflowSecretName)
	}
	dst := filepath.Join(c.opt.Out, workflowSecretName)
	if same(src, dst) {
		return dst, os.Chmod(dst, 0o600)
	}
	raw, err := os.ReadFile(src)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(dst); err == nil {
		return "", errors.New(dst + " already exists")
	}
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return "", err
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		return "", err
	}
	return dst, os.Rename(tmp, dst)
}

func same(a, b string) bool {
	sa, err1 := os.Stat(a)
	sb, err2 := os.Stat(b)
	return err1 == nil && err2 == nil && os.SameFile(sa, sb)
}
