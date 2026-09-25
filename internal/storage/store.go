package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/h3yow/fleet-task-dispatcher-go/internal/domain"
)

// Store 是本地持久化存储抽象。所有方法都必须保证并发安全；
// 写操作必须在返回前落盘（原子替换），以便进程中断后能恢复到一致状态。
type Store interface {
	Load() (*domain.State, error)
	Save(state *domain.State) error
	AppendDecision(d domain.Decision) error
	LoadDecisions() ([]domain.Decision, error)
}

// FileStore 用单个 JSON 文件保存状态快照，另用 JSON Lines 文件追加判定日志。
// statePath/logPath 为空时退化为纯内存模式（用于不想触碰文件系统的单元测试）。
type FileStore struct {
	mu        sync.Mutex
	statePath string
	logPath   string

	memState     *domain.State
	memDecisions []domain.Decision
}

// NewFileStore 创建/打开一个文件存储。
func NewFileStore(statePath, logPath string) *FileStore {
	return &FileStore{statePath: statePath, logPath: logPath}
}

// Load 读取最近一次持久化的全量状态；文件不存在时返回空状态（全新启动）。
func (s *FileStore) Load() (*domain.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.statePath == "" {
		if s.memState == nil {
			s.memState = newState()
		}
		return cloneState(s.memState), nil
	}

	data, err := os.ReadFile(s.statePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return newState(), nil
		}
		return nil, fmt.Errorf("load state: %w", err)
	}
	if len(data) == 0 {
		return newState(), nil
	}
	var st domain.State
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	if st.Tasks == nil {
		st.Tasks = map[string]*domain.Task{}
	}
	if st.Robots == nil {
		st.Robots = map[string]*domain.Robot{}
	}
	return &st, nil
}

// Save 以"临时文件 + rename"原子替换方式持久化全量快照。
func (s *FileStore) Save(state *domain.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.statePath == "" {
		s.memState = cloneState(state)
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(s.statePath), 0o755); err != nil {
		return fmt.Errorf("mkdir state: %w", err)
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	tmp := s.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write temp state: %w", err)
	}
	if err := os.Rename(tmp, s.statePath); err != nil {
		return fmt.Errorf("rename state: %w", err)
	}
	return nil
}

// AppendDecision 向判定日志追加一行 JSON（O_APPEND，崩溃时最多丢最后一行，不影响状态恢复）。
func (s *FileStore) AppendDecision(d domain.Decision) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.logPath == "" {
		s.memDecisions = append(s.memDecisions, d)
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(s.logPath), 0o755); err != nil {
		return fmt.Errorf("mkdir log: %w", err)
	}
	f, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open decision log: %w", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	if err := enc.Encode(d); err != nil {
		return fmt.Errorf("append decision: %w", err)
	}
	return nil
}

// LoadDecisions 读取全部判定日志。
func (s *FileStore) LoadDecisions() ([]domain.Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.logPath == "" {
		out := make([]domain.Decision, len(s.memDecisions))
		copy(out, s.memDecisions)
		return out, nil
	}

	data, err := os.ReadFile(s.logPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read decision log: %w", err)
	}
	var out []domain.Decision
	for len(data) > 0 {
		var d domain.Decision
		if err := json.Unmarshal(popLine(&data), &d); err != nil {
			return nil, fmt.Errorf("decode decision: %w", err)
		}
		out = append(out, d)
	}
	return out, nil
}

func popLine(data *[]byte) []byte {
	for i, b := range *data {
		if b == '\n' {
			line := (*data)[:i]
			*data = (*data)[i+1:]
			return line
		}
	}
	line := *data
	*data = nil
	return line
}

func newState() *domain.State {
	return &domain.State{
		Tasks:  map[string]*domain.Task{},
		Robots: map[string]*domain.Robot{},
	}
}

func cloneState(st *domain.State) *domain.State {
	cp := newState()
	for k, v := range st.Tasks {
		t := *v
		if v.Dependencies != nil {
			t.Dependencies = append([]string(nil), v.Dependencies...)
		}
		cp.Tasks[k] = &t
	}
	for k, v := range st.Robots {
		r := *v
		cp.Robots[k] = &r
	}
	return cp
}
