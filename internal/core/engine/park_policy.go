package engine

import (
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

const parkUsage = "Usage: `helper park [on|off|status]` in a work chat, or `park <session> [on|off|status]` in the control chat. Without on/off/status, park immediately."

// cmdParkPolicy extends park without changing the existing manual forms.
func (e *Engine) cmdParkPolicy(j cmdJob, cs *convState, arg string) {
	fields := strings.Fields(arg)
	var target *convState
	action := ""
	if cs.kind == store.KindWork && len(fields) == 1 && parkAction(fields[0]) {
		target, action = cs, strings.ToLower(fields[0])
	} else if len(fields) == 2 && parkAction(fields[1]) {
		target, action = e.findLabel(fields[0]), strings.ToLower(fields[1])
	} else if len(fields) == 0 && cs.kind == store.KindWork {
		e.cmdPark(j, cs)
		return
	} else if len(fields) == 1 && !parkAction(fields[0]) {
		e.cmdParkLabel(j, fields[0])
		return
	} else {
		e.reply(j, parkUsage, nil)
		return
	}
	if target == nil || target.id == e.inboxID {
		e.reply(j, "No work session found. Send `list` to see sessions.", nil)
		return
	}
	e.parkPolicyMu.Lock()
	defer e.parkPolicyMu.Unlock()
	var saved store.Conversation
	err := e.replyIn(j, func(tx *store.Tx) (string, error) {
		var err error
		saved, err = parkPolicy(tx, target.id, action)
		if err != nil {
			return "", err
		}
		return parkPolicyText(saved), nil
	})
	if err == nil {
		e.publishParkPolicy(saved)
	}
}

func parkAction(s string) bool {
	switch strings.ToLower(s) {
	case "on", "off", "status":
		return true
	}
	return false
}

func parkPolicy(tx *store.Tx, id int64, action string) (store.Conversation, error) {
	if action != "status" {
		if err := tx.SetAutoPark(id, action == "on"); err != nil {
			return store.Conversation{}, err
		}
	}
	c, ok, err := tx.Conversation(id)
	if err == nil && !ok {
		err = store.ErrNotFound
	}
	return c, err
}

func (e *Engine) publishParkPolicy(c store.Conversation) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if cs := e.convs[c.ID]; cs != nil {
		cs.autoParkOff = c.AutoParkOff
	}
}

func autoParkValue(off bool) string {
	if off {
		return "off"
	}
	return "on"
}

func parkPolicyText(c store.Conversation) string {
	text := fmt.Sprintf("%s: Auto park: %s. Chat state: %s.", c.Label, autoParkValue(c.AutoParkOff), c.State)
	if c.AutoParkOff {
		text += " This setting survives helper restarts. When active and cooled down, this chat is read every 30 seconds, subject to read pacing and backoff. Manual park is still available."
	} else {
		text += " Idle chats can be parked automatically after the normal inactivity timeout."
	}
	if c.State == store.ConvParked {
		text += fmt.Sprintf(" The chat remains parked. Send `r %s` in the control chat to wake it; queued messages can then run.", c.Label)
	} else if c.State != store.ConvActive {
		text += " This setting does not activate the chat."
	}
	return text
}

func (e *Engine) cliParkPolicy(r CLIRequest) (CLIResult, error) {
	if !parkAction(r.Message) {
		return cliFail("park requires on, off, or status")
	}
	target, msg := e.cliSession(r.Selector)
	if target == nil {
		return cliFail("%s", msg)
	}
	e.parkPolicyMu.Lock()
	defer e.parkPolicyMu.Unlock()
	var c store.Conversation
	apply := func(tx *store.Tx) (err error) { c, err = parkPolicy(tx, target.id, strings.ToLower(r.Message)); return }
	var err error
	if strings.EqualFold(r.Message, "status") {
		err = e.st.Read(apply)
	} else {
		err = e.update(apply)
	}
	if err != nil {
		return CLIResult{}, err
	}
	e.publishParkPolicy(c)
	return CLIResult{Lines: []string{parkPolicyText(c)}}, nil
}
