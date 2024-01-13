package uraft

import (
	"sort"
)

func min(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a int64, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

type int64Sort []int64

func (s int64Sort) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s int64Sort) Len() int {
	return len(s)
}

func (s int64Sort) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func int64Sorts(s []int64) {
	sort.Sort(int64Sort(s))
}
