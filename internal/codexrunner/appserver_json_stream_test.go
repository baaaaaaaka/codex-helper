package codexrunner

import "testing"

func TestAppServerTopLevelJSONScannerFindsRequestFieldsAcrossChunkBoundaries(t *testing.T) {
	raw := []byte(` {"params":{"nested":[{"text":"brace } and quote \\\""}]},"id":17,"met\u0068od":"item/commandExecution/requestApproval"} `)
	var scanner appServerTopLevelJSONScanner
	var fields []string
	var id, method []byte
	for _, value := range raw {
		event := scanner.FeedByte(value)
		if event.Invalid {
			t.Fatalf("scanner rejected valid JSON at byte %q", value)
		}
		if event.FieldComplete != "" {
			fields = append(fields, event.FieldComplete)
			switch event.FieldComplete {
			case "id":
				id = append([]byte(nil), event.RawValue...)
			case "method":
				method = append([]byte(nil), event.RawValue...)
			}
		}
	}
	if !scanner.Complete() {
		t.Fatal("scanner did not finish the top-level object")
	}
	if len(fields) != 3 || fields[0] != "params" || fields[1] != "id" || fields[2] != "method" {
		t.Fatalf("completed fields = %v", fields)
	}
	if string(id) != "17" {
		t.Fatalf("id raw value = %q, want 17", id)
	}
	decodedMethod, ok := decodeAppServerMethod(method)
	if !ok || decodedMethod != appServerMethodCommandExecutionApproval {
		t.Fatalf("decoded method = %q, ok=%t", decodedMethod, ok)
	}
}

func TestAppServerTopLevelJSONScannerStopsAtLargeResultField(t *testing.T) {
	var scanner appServerTopLevelJSONScanner
	for _, value := range []byte(`{"id":3,"result":`) {
		event := scanner.FeedByte(value)
		if event.FieldStart == "result" {
			return
		}
		if event.Invalid {
			t.Fatalf("scanner rejected response header at byte %q", value)
		}
	}
	t.Fatal("scanner did not identify result before consuming its value")
}

func TestAppServerTopLevelJSONScannerRejectsMalformedEnvelope(t *testing.T) {
	for _, raw := range []string{
		`[]`,
		`{"id":1,}`,
		`{"id":1} trailing`,
	} {
		t.Run(raw, func(t *testing.T) {
			var scanner appServerTopLevelJSONScanner
			invalid := false
			for _, value := range []byte(raw) {
				if scanner.FeedByte(value).Invalid {
					invalid = true
					break
				}
			}
			if !invalid {
				t.Fatalf("scanner accepted malformed envelope %q", raw)
			}
		})
	}
}
