package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func (s *Store) ListRuns(since time.Time) ([]RunRecord, error) {
	s.mu.Lock()
	s.currentFile.Sync()
	s.mu.Unlock()

	files, err := filepath.Glob(filepath.Join(s.dataDir, "runs*.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("glob runs: %w", err)
	}
	sort.Strings(files)

	var runs []RunRecord
	for _, f := range files {
		records, err := readJSONL(f, since)
		if err != nil {
			continue
		}
		runs = append(runs, records...)
	}

	// Most recent first
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].StartedAt.After(runs[j].StartedAt)
	})

	return runs, nil
}

func readJSONL(path string, since time.Time) ([]RunRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var records []RunRecord
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var r RunRecord
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			continue
		}
		if r.StartedAt.After(since) || r.StartedAt.Equal(since) {
			records = append(records, r)
		}
	}
	return records, scanner.Err()
}
