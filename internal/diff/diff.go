// Package diff computes a line diff with the longest-common-subsequence
// method. It is quadratic, so inputs are capped; webhook payloads are small.
package diff

// Op is the kind of a diff line.
type Op int

const (
	Equal Op = iota
	Delete
	Insert
)

// Line is one line of output.
type Line struct {
	Op   Op
	Text string
}

// MaxLines caps each side; beyond that the tail is compared as one block.
const MaxLines = 2000

// Lines diffs a against b.
func Lines(a, b []string) []Line {
	if len(a) > MaxLines {
		a = append(a[:MaxLines:MaxLines], "… (truncated)")
	}
	if len(b) > MaxLines {
		b = append(b[:MaxLines:MaxLines], "… (truncated)")
	}
	// Trim the common prefix and suffix: cheap and very common.
	pre := 0
	for pre < len(a) && pre < len(b) && a[pre] == b[pre] {
		pre++
	}
	suf := 0
	for suf < len(a)-pre && suf < len(b)-pre && a[len(a)-1-suf] == b[len(b)-1-suf] {
		suf++
	}
	var out []Line
	for _, s := range a[:pre] {
		out = append(out, Line{Equal, s})
	}
	out = append(out, lcs(a[pre:len(a)-suf], b[pre:len(b)-suf])...)
	for _, s := range a[len(a)-suf:] {
		out = append(out, Line{Equal, s})
	}
	return out
}

func lcs(a, b []string) []Line {
	n, m := len(a), len(b)
	// t[i][j] = LCS length of a[i:], b[j:]
	t := make([][]int, n+1)
	for i := range t {
		t[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				t[i][j] = t[i+1][j+1] + 1
			} else {
				t[i][j] = max(t[i+1][j], t[i][j+1])
			}
		}
	}
	var out []Line
	i, j := 0, 0
	for i < n && j < m {
		switch {
		case a[i] == b[j]:
			out = append(out, Line{Equal, a[i]})
			i++
			j++
		case t[i+1][j] >= t[i][j+1]:
			out = append(out, Line{Delete, a[i]})
			i++
		default:
			out = append(out, Line{Insert, b[j]})
			j++
		}
	}
	for ; i < n; i++ {
		out = append(out, Line{Delete, a[i]})
	}
	for ; j < m; j++ {
		out = append(out, Line{Insert, b[j]})
	}
	return out
}
