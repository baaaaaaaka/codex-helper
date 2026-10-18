package codexrunner

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type policyInitializeRecordMode uint8

const (
	policyInitializeRecordUndecided policyInitializeRecordMode = iota
	policyInitializeRecordPassthrough
	policyInitializeRecordCapture
)

type policyInitializeResponseObserver struct {
	transport *policyAppServerTransport
	write     func([]byte) error
	mode      policyInitializeRecordMode
	scanner   appServerTopLevelJSONScanner
	prefix    []byte
	captured  []byte
}

func newPolicyInitializeResponseObserver(transport *policyAppServerTransport, write func([]byte) error) *policyInitializeResponseObserver {
	return &policyInitializeResponseObserver{transport: transport, write: write}
}

func (o *policyInitializeResponseObserver) WriteChunk(chunk []byte) error {
	for index := 0; index < len(chunk); index++ {
		switch o.mode {
		case policyInitializeRecordPassthrough:
			return o.write(chunk[index:])
		case policyInitializeRecordCapture:
			return o.appendCapture(chunk[index:])
		}

		o.prefix = append(o.prefix, chunk[index])
		event := o.scanner.FeedByte(chunk[index])
		if event.Invalid {
			o.mode = policyInitializeRecordPassthrough
			if err := o.write(o.prefix); err != nil {
				return err
			}
			o.prefix = nil
			return o.write(chunk[index+1:])
		}
		if event.FieldComplete == "id" {
			if o.transport.isPendingInitializeID(event.RawValue) {
				o.mode = policyInitializeRecordCapture
				o.captured = append(o.captured[:0], o.prefix...)
				o.prefix = nil
				if len(o.captured) > defaultRemoteBrokerApprovalCaptureLimit {
					return fmt.Errorf("initialize response exceeded %d byte capture limit", defaultRemoteBrokerApprovalCaptureLimit)
				}
			} else {
				o.mode = policyInitializeRecordPassthrough
				if err := o.write(o.prefix); err != nil {
					return err
				}
				o.prefix = nil
				return o.write(chunk[index+1:])
			}
		}
		if o.mode == policyInitializeRecordUndecided && (o.scanner.Invalid() || o.scanner.Complete() || len(o.prefix) >= defaultRemoteBrokerClassificationPrefixLimit) {
			o.mode = policyInitializeRecordPassthrough
			if err := o.write(o.prefix); err != nil {
				return err
			}
			o.prefix = nil
			return o.write(chunk[index+1:])
		}
	}
	return nil
}

func (o *policyInitializeResponseObserver) appendCapture(chunk []byte) error {
	if len(chunk) == 0 {
		return nil
	}
	if len(o.captured)+len(chunk) > defaultRemoteBrokerApprovalCaptureLimit {
		return fmt.Errorf("initialize response exceeded %d byte capture limit", defaultRemoteBrokerApprovalCaptureLimit)
	}
	o.captured = append(o.captured, chunk...)
	return nil
}

func (o *policyInitializeResponseObserver) Finish() error {
	if o.mode == policyInitializeRecordUndecided {
		o.mode = policyInitializeRecordPassthrough
		if err := o.write(o.prefix); err != nil {
			return err
		}
		o.prefix = nil
	}
	if o.mode != policyInitializeRecordCapture {
		return nil
	}

	var message appServerMessage
	if json.Unmarshal(bytes.TrimSpace(o.captured), &message) == nil && message.Error == nil {
		key := canonicalAppServerID(message.ID)
		o.transport.protocolMu.Lock()
		_, ready := o.transport.initializeIDs[key]
		if ready {
			delete(o.transport.initializeIDs, key)
		}
		o.transport.protocolMu.Unlock()
		if ready && o.transport.readyHook != nil {
			o.transport.readyOnce.Do(func() { o.transport.readyErr = o.transport.readyHook() })
			if o.transport.readyErr != nil {
				return o.transport.readyErr
			}
		}
	}
	return o.write(o.captured)
}

func (t *policyAppServerTransport) isPendingInitializeID(raw []byte) bool {
	key := canonicalAppServerID(raw)
	if key == "" {
		return false
	}
	t.protocolMu.Lock()
	_, ok := t.initializeIDs[key]
	t.protocolMu.Unlock()
	return ok
}
