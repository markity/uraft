package structs

type LogEntry struct {
	LogTerm      int64
	LogIndex     int64
	CommandType  string
	CommandBytes []byte
}

type Logs []LogEntry

func (l *Logs) LastLog() LogEntry {
	return (*l)[len(*l)-1]
}

// 0 1 2 3 4 5
// 1 2 3 4 5 6
func (l *Logs) FindLogByIndex(idx int64) (LogEntry, bool) {
	realIdx := idx - (*l)[0].LogIndex
	if realIdx < 0 || int64(len(*l)-1) < realIdx {
		return LogEntry{}, false
	}
	return (*l)[realIdx], true
}

func (l *Logs) At(id int64) LogEntry {
	return (*l)[id]
}

// 如果日志为空, 返回lastIncludedIndex
func (l *Logs) LastLogIndex() int64 {
	return (*l)[len(*l)-1].LogIndex
}

// 4 5 6 7 8
func (l *Logs) GetByIndex(idx int64) LogEntry {
	return (*l)[idx-(*l)[0].LogIndex]
}

// 剪切日志, 但是包含那个日志, 把idx对应日志的command变为空
func (l *Logs) TrimLogs(idx int64) {
	i := l.GetByIndex(idx).LogIndex - (*l)[0].LogIndex
	(*l) = (*l)[i:]
	(*l)[0].CommandType = ""
}

// 截断后面的指定id以及后面的所有日志
func (l *Logs) TruncateBy(id int64) {
	*l = (*l)[:id-(*l)[0].LogIndex]
}

func (l *Logs) Append(e LogEntry) {
	e.LogIndex = l.LastLog().LogIndex + 1
	*l = append(*l, e)
}

func (l *Logs) Copy() Logs {
	lo := Logs{}
	lo = append(lo, *l...)
	return lo
}
