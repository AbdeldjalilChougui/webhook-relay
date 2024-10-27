package diff

import (
	"strings"
	"testing"
)

func render(ls []Line) string {
	var sb strings.Builder
	for _, l := range ls {
		sb.WriteString([]string{" ", "-", "+"}[l.Op] + l.Text + "\n")
	}
	return sb.String()
}

func TestLines(t *testing.T) {
	a := strings.Split("POST /hook\nA: 1\nB: 2\n\n{\n  \"x\": 1\n}", "\n")
	b := strings.Split("POST /hook\nA: 1\nC: 3\n\n{\n  \"x\": 2\n}", "\n")
	want := " POST /hook\n A: 1\n-B: 2\n+C: 3\n \n {\n-  \"x\": 1\n+  \"x\": 2\n }\n"
	if got := render(Lines(a, b)); got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
	if got := render(Lines(a, a)); strings.ContainsAny(got[:1], "+-") {
		t.Fatal("identical inputs produced changes")
	}
	if got := Lines(nil, []string{"x"}); len(got) != 1 || got[0].Op != Insert {
		t.Fatalf("%v", got)
	}
}
