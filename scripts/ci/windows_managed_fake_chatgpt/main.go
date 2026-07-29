// Command windows-managed-fake-chatgpt is used only by the credential-free
// Windows CI smoke. It records launch inheritance and performs a nonce HTTP
// request through HTTP_PROXY, then stays alive long enough for the parent test
// to inspect its process path.
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	nonce := os.Getenv("CXP_TEST_NONCE")
	logPath := os.Getenv("CXP_TEST_CHILD_LOG")
	if strings.TrimSpace(logPath) == "" {
		panic("CXP_TEST_CHILD_LOG is required")
	}
	lines := []string{
		"args=" + strings.Join(os.Args[1:], " "),
		"cwd=" + must(os.Getwd()),
		"CODEX_HOME=" + os.Getenv("CODEX_HOME"),
		"CODEX_DIR=" + os.Getenv("CODEX_DIR"),
		"HTTP_PROXY=" + os.Getenv("HTTP_PROXY"),
		"HTTPS_PROXY=" + os.Getenv("HTTPS_PROXY"),
		"ALL_PROXY=" + os.Getenv("ALL_PROXY"),
		"NO_PROXY=" + os.Getenv("NO_PROXY"),
		"no_proxy=" + os.Getenv("no_proxy"),
		"CXP_RUNTIME=" + os.Getenv("CXP_RUNTIME"),
	}
	response, err := http.Get("http://cxp-managed-child.invalid/nonce/" + nonce)
	if err != nil {
		lines = append(lines, "request_error="+err.Error())
	} else {
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		lines = append(lines, fmt.Sprintf("request_status=%d", response.StatusCode), "request_body="+string(body))
		if readErr != nil {
			lines = append(lines, "request_error="+readErr.Error())
		}
	}
	if err := os.WriteFile(logPath, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		panic(err)
	}
	time.Sleep(20 * time.Second)
}

func must(value string, err error) string {
	if err != nil {
		panic(err)
	}
	return value
}
