package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
	"github.com/baaaaaaaka/codex-helper/internal/teams/machineregistry"
)

type machineCard = machineregistry.MachineCard
type machineStatus = machineregistry.MachineStatus
type machineRegistryMessage = machineregistry.ChatMessage

type probeSummary struct {
	Event        string          `json:"event"`
	RegistryKey  string          `json:"registry_key"`
	ExternalID   string          `json:"external_id"`
	ChatIDHash   string          `json:"chat_id_hash"`
	SlotIDHash   string          `json:"slot_id_hash,omitempty"`
	PublishMode  string          `json:"publish_mode,omitempty"`
	MeetingID    string          `json:"meeting_id,omitempty"`
	MachineID    string          `json:"machine_id,omitempty"`
	Online       []machineStatus `json:"online,omitempty"`
	Stale        []machineStatus `json:"stale,omitempty"`
	Observed     []machineStatus `json:"observed,omitempty"`
	PublishedSeq int             `json:"published_seq,omitempty"`
}

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "teams-registry-probe: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("teams-registry-probe", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	registryKey := fs.String("registry-key", strings.TrimSpace(os.Getenv("CXP_REGISTRY_PROBE_KEY")), "shared registry key for this probe")
	machineID := fs.String("machine", strings.TrimSpace(os.Getenv("CXP_REGISTRY_MACHINE_ID")), "machine id to publish")
	role := fs.String("role", "both", "publisher, watcher, or both")
	heartbeat := fs.Duration("heartbeat", 15*time.Second, "publish interval")
	ttl := fs.Duration("ttl", 45*time.Second, "online TTL")
	duration := fs.Duration("duration", 1*time.Minute, "total probe duration")
	pageSize := fs.Int("page-size", 50, "Graph message page size, 1-50; pagination follows nextLink until the registry window is exhausted")
	topAlias := fs.Int("top", 0, "deprecated alias for --page-size")
	if err := fs.Parse(args); err != nil {
		return err
	}
	probeRole, err := normalizeProbeRole(*role)
	if err != nil {
		return err
	}
	key := strings.TrimSpace(*registryKey)
	if key == "" {
		return fmt.Errorf("--registry-key is required")
	}
	id := strings.TrimSpace(*machineID)
	if id == "" {
		host, _ := os.Hostname()
		id = strings.TrimSpace(host)
	}
	if id == "" && canPublish(probeRole) {
		return fmt.Errorf("--machine is required for publisher role")
	}
	if *heartbeat <= 0 {
		return fmt.Errorf("--heartbeat must be positive")
	}
	if *ttl <= *heartbeat {
		return fmt.Errorf("--ttl must be greater than --heartbeat")
	}
	if *duration <= 0 {
		return fmt.Errorf("--duration must be positive")
	}
	readPageSize, err := registryReadPageSize(*pageSize, *topAlias)
	if err != nil {
		return err
	}
	cfg, err := teams.DefaultEffectiveAuthConfig()
	if err != nil {
		return err
	}
	graph := teams.NewGraphClient(teams.NewAuthManager(cfg), io.Discard)
	registryGraph := teams.NewMachineRegistryGraphAdapter(graph)
	externalID := registryExternalID(key)
	subject := "CXP Registry Probe " + shortHash(key)
	now := time.Now().UTC()
	chat, meeting, err := registryGraph.CreateOrGetMeetingChatWindow(ctx, subject, externalID, now.Add(-5*time.Minute), now.Add(machineregistry.DefaultWindowDuration))
	if err != nil {
		return err
	}
	writeSummary(probeSummary{
		Event:       "registry",
		RegistryKey: key,
		ExternalID:  externalID,
		ChatIDHash:  shortHash(chat.ID),
		MeetingID:   meeting.ID,
		MachineID:   id,
	})

	publisher := machineregistry.HeartbeatPublisher{Graph: registryGraph, ChatID: chat.ID}
	deadline := time.Now().Add(*duration)
	nextPublish := time.Time{}
	nextObserve := time.Time{}
	seq := 0
	for {
		now := time.Now()
		if !now.Before(deadline) {
			break
		}
		if canPublish(probeRole) && (nextPublish.IsZero() || !now.Before(nextPublish)) {
			seq++
			card := machineCard{
				Kind:                     machineregistry.CardKind,
				RegistryKey:              key,
				MachineID:                id,
				MachineLabel:             id,
				Capabilities:             []string{"docker", "teams-registry-probe"},
				Sequence:                 seq,
				HeartbeatIntervalSeconds: int(heartbeat.Seconds()),
				TTLSeconds:               int(ttl.Seconds()),
				PublishedAt:              now.UTC().Format(time.RFC3339Nano),
				ExpiresAt:                now.Add(*ttl).UTC().Format(time.RFC3339Nano),
			}
			result, err := publisher.Publish(ctx, card)
			if err != nil {
				return fmt.Errorf("publish heartbeat: %w", err)
			}
			writeSummary(probeSummary{
				Event:        "published",
				RegistryKey:  key,
				ExternalID:   externalID,
				ChatIDHash:   shortHash(chat.ID),
				SlotIDHash:   shortHash(result.SlotMessageID),
				PublishMode:  result.Mode,
				MachineID:    id,
				PublishedSeq: seq,
			})
			nextPublish = now.Add(*heartbeat)
		}
		if canWatch(probeRole) && (nextObserve.IsZero() || !now.Before(nextObserve)) {
			statuses, err := machineregistry.Observe(ctx, registryGraph, chat.ID, key, readPageSize, now)
			if err != nil {
				return fmt.Errorf("observe registry: %w", err)
			}
			online, stale := machineregistry.SplitStatuses(statuses)
			writeSummary(probeSummary{
				Event:       "observed",
				RegistryKey: key,
				ExternalID:  externalID,
				ChatIDHash:  shortHash(chat.ID),
				MachineID:   id,
				Online:      online,
				Stale:       stale,
				Observed:    statuses,
			})
			nextObserve = now.Add(*heartbeat)
		}
		sleep := minDuration(time.Until(deadline), time.Until(nextPublish), time.Until(nextObserve), time.Second)
		if sleep <= 0 {
			sleep = 200 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
	}
	if canWatch(probeRole) {
		statuses, err := machineregistry.Observe(ctx, registryGraph, chat.ID, key, readPageSize, time.Now())
		if err != nil {
			return fmt.Errorf("observe registry final: %w", err)
		}
		online, stale := machineregistry.SplitStatuses(statuses)
		writeSummary(probeSummary{
			Event:       "final",
			RegistryKey: key,
			ExternalID:  externalID,
			ChatIDHash:  shortHash(chat.ID),
			MachineID:   id,
			Online:      online,
			Stale:       stale,
			Observed:    statuses,
		})
	}
	return nil
}

func normalizeProbeRole(role string) (string, error) {
	role = strings.TrimSpace(strings.ToLower(role))
	switch role {
	case "publisher", "watcher", "both":
		return role, nil
	default:
		return "", fmt.Errorf("--role must be publisher, watcher, or both")
	}
}

func canPublish(role string) bool {
	role = strings.TrimSpace(strings.ToLower(role))
	return role == "publisher" || role == "both"
}

func canWatch(role string) bool {
	role = strings.TrimSpace(strings.ToLower(role))
	return role == "watcher" || role == "both"
}

type heartbeatPublisher struct {
	graph         machineregistry.HeartbeatGraph
	chatID        string
	slotMessageID string
}

type heartbeatPublishResult struct {
	slotMessageID string
	mode          string
}

func (p *heartbeatPublisher) publish(ctx context.Context, card machineCard) (heartbeatPublishResult, error) {
	publisher := machineregistry.HeartbeatPublisher{
		Graph:         p.graph,
		ChatID:        p.chatID,
		SlotMessageID: p.slotMessageID,
	}
	result, err := publisher.Publish(ctx, card)
	if err != nil {
		return heartbeatPublishResult{}, err
	}
	p.slotMessageID = publisher.SlotMessageID
	return heartbeatPublishResult{slotMessageID: result.SlotMessageID, mode: result.Mode}, nil
}

func registryExternalID(key string) string {
	return machineregistry.ExternalID("probe:" + key)
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:6])
}

func renderCardHTML(card machineCard) string {
	return machineregistry.RenderCardHTML(card)
}

func observe(ctx context.Context, graph machineregistry.MessageLister, chatID string, registryKey string, top int, now time.Time) ([]machineStatus, error) {
	return machineregistry.Observe(ctx, graph, chatID, registryKey, top, now)
}

func parseCardMessage(content string) (machineCard, bool) {
	return machineregistry.ParseCardMessage(content)
}

func splitStatuses(statuses []machineStatus) ([]machineStatus, []machineStatus) {
	return machineregistry.SplitStatuses(statuses)
}

func registryReadPageSize(pageSize int, topAlias int) (int, error) {
	const maxPageSize = 50
	readPageSize := pageSize
	if topAlias > 0 {
		readPageSize = topAlias
	}
	if readPageSize <= 0 {
		return 0, fmt.Errorf("--page-size must be positive")
	}
	if readPageSize > maxPageSize {
		return 0, fmt.Errorf("--page-size must be <= %d", maxPageSize)
	}
	return readPageSize, nil
}

func minDuration(values ...time.Duration) time.Duration {
	var min time.Duration
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if min == 0 || value < min {
			min = value
		}
	}
	return min
}

func writeSummary(summary probeSummary) {
	raw, err := json.Marshal(summary)
	if err != nil {
		fmt.Printf(`{"event":"marshal_error","error":%q}`+"\n", err.Error())
		return
	}
	fmt.Println(string(raw))
}
