package etcdplugin

import (
	"os"
	"path/filepath"
	"testing"
)

func writeNames(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "names")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("could not write names file: %v", err)
	}
	return path
}

func TestLoadNames(t *testing.T) {
	path := writeNames(t, `# a comment
static printer aa:bb:cc:dd:ee:01
alias www webserver

static nas AA-BB-CC-DD-EE-02
`)

	static, aliases, err := LoadNames(path)
	if err != nil {
		t.Fatalf("could not load names: %v", err)
	}

	// hardware addresses are normalised, whatever separator the file used
	if got := static["aa:bb:cc:dd:ee:01"]; got != "printer" {
		t.Errorf("static[aa:bb:cc:dd:ee:01] = %q, want printer", got)
	}
	if got := static["aa:bb:cc:dd:ee:02"]; got != "nas" {
		t.Errorf("dash separated macs are not normalised: got %q", got)
	}
	if len(static) != 2 {
		t.Errorf("loaded %d static entries, want 2", len(static))
	}

	if got := aliases["www"]; got != "webserver" {
		t.Errorf("aliases[www] = %q, want webserver", got)
	}
	if len(aliases) != 1 {
		t.Errorf("loaded %d aliases, want 1", len(aliases))
	}
}

// Names become etcd keys, and the reader (coredns-etcd, since 3f6a5d2a) only
// ever looks up the lowercase form. A name stored with any uppercase in it is
// therefore a record that exists and can never be resolved -- which is exactly
// how 17 of 22 leases went dark here while sitting in etcd in plain sight.
func TestLoadNamesLowercasesNames(t *testing.T) {
	path := writeNames(t, `static S380HB aa:bb:cc:dd:ee:03
alias EVCS EVCS-HQ26026HJMQ
`)

	static, aliases, err := LoadNames(path)
	if err != nil {
		t.Fatalf("could not load names: %v", err)
	}

	if got := static["aa:bb:cc:dd:ee:03"]; got != "s380hb" {
		t.Errorf("static name not lowercased: got %q, want s380hb", got)
	}
	// The alias map is keyed on the DHCP-supplied hostname, which Register
	// lowercases before looking in here -- so an uppercase key never matches.
	if got := aliases["evcs"]; got != "evcs-hq26026hjmq" {
		t.Errorf("alias not lowercased: aliases[evcs] = %q, want evcs-hq26026hjmq", got)
	}
	if _, ok := aliases["EVCS"]; ok {
		t.Error("alias retained its uppercase key, which Register can never match")
	}
}

func TestLoadNamesRejectsMalformedInput(t *testing.T) {
	for name, body := range map[string]string{
		"too few fields":    "static printer\n",
		"too many fields":   "static printer aa:bb:cc:dd:ee:01 extra\n",
		"bad hardware addr": "static printer not-a-mac\n",
		"trailing garbage":  "alias www webserver\nnonsense\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := LoadNames(writeNames(t, body)); err == nil {
				t.Error("malformed names file was accepted")
			}
		})
	}
}

func TestLoadNamesIgnoresUnknownDirectives(t *testing.T) {
	// three fields, but the first is not a directive we know about
	static, aliases, err := LoadNames(writeNames(t, "reserved printer aa:bb:cc:dd:ee:01\n"))
	if err != nil {
		t.Fatalf("could not load names: %v", err)
	}
	if len(static) != 0 || len(aliases) != 0 {
		t.Errorf("unknown directive produced entries: %v %v", static, aliases)
	}
}

func TestLoadNamesMissingFile(t *testing.T) {
	if _, _, err := LoadNames(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Error("a missing names file was accepted")
	}
}
