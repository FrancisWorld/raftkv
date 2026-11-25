package main

import "testing"

func TestParsePeers(t *testing.T) {
	got, err := parsePeers("1=http://a:8080, 2=http://b:8080/,3=https://c")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[2] != "http://b:8080" || got[3] != "https://c" {
		t.Fatalf("peers = %v", got)
	}
	for _, bad := range []string{"", "1", "x=http://a", "0=http://a", "1=ftp://a", "1=http://a,1=http://b"} {
		if _, err := parsePeers(bad); err == nil {
			t.Errorf("parsePeers(%q) should fail", bad)
		}
	}
}

func TestParseFlags(t *testing.T) {
	if _, err := parseFlags(nil); err == nil {
		t.Fatal("missing --id should fail")
	}
	o, err := parseFlags([]string{"--id", "2", "--tick", "50ms", "--snapshot-threshold", "0"})
	if err != nil || o.id != 2 || o.snapshotThreshold != 0 {
		t.Fatalf("options = %+v, err = %v", o, err)
	}
	if _, err := parseFlags([]string{"--id", "1", "--tick", "0s"}); err == nil {
		t.Fatal("zero tick should fail")
	}
}

func TestRunRejectsUnknownSelf(t *testing.T) {
	if err := run([]string{"--id", "9", "--peers", "1=http://a,2=http://b"}); err == nil {
		t.Fatal("expected an error when --id is not in --peers")
	}
}
