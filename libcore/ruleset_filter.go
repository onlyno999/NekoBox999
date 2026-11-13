package libcore

import (
	"bytes"
	"os"
	"strings"

	"github.com/sagernet/sing-box/common/srs"
	"github.com/sagernet/sing-box/option"
)

// Google-operated hosts must never be claimed by the CN
// domain rule set: Play Store downloads ride dl.google.com,
// googleapis and the gvt/gstatic hosts, and sending them to
// the direct path stalls downloads near completion. The CN
// geosite data (V2 open data) still carries such entries, so
// the app filters its downloaded copy before the core uses
// it. Matching is by substring on the entry text, which
// covers full domains, suffixes, keywords and regexes alike.
var googleHostTokens = []string{
	"google", "gstatic", "gvt", "ggpht", "youtube", "doubleclick",
}

func isGoogleHostEntry(value string) bool {
	lower := strings.ToLower(value)
	for _, token := range googleHostTokens {
		if strings.Contains(lower, token) {
			return true
		}
	}
	return false
}

func filterHostEntries(values []string) []string {
	kept := make([]string, 0, len(values))
	for _, value := range values {
		if !isGoogleHostEntry(value) {
			kept = append(kept, value)
		}
	}
	return kept
}

// FilterCnRuleSet reads the binary rule set at srcPath, drops
// every Google-operated host entry from its domain lists and
// writes the filtered rule set to destPath. Rules left with
// no conditions at all are dropped entirely.
func FilterCnRuleSet(srcPath string, destPath string) error {
	data, err := os.ReadFile(srcPath)
	if err != nil {
		return err
	}
	compat, err := srs.Read(bytes.NewReader(data), true)
	if err != nil {
		return err
	}
	plain, err := compat.Upgrade()
	if err != nil {
		return err
	}
	rules := make([]option.HeadlessRule, 0, len(plain.Rules))
	for _, rule := range plain.Rules {
		if rule.Type == "" || rule.Type == "default" {
			rule.DefaultOptions.Domain = filterHostEntries(rule.DefaultOptions.Domain)
			rule.DefaultOptions.DomainSuffix = filterHostEntries(rule.DefaultOptions.DomainSuffix)
			rule.DefaultOptions.DomainKeyword = filterHostEntries(rule.DefaultOptions.DomainKeyword)
			rule.DefaultOptions.DomainRegex = filterHostEntries(rule.DefaultOptions.DomainRegex)
			if len(rule.DefaultOptions.Domain) == 0 &&
				len(rule.DefaultOptions.DomainSuffix) == 0 &&
				len(rule.DefaultOptions.DomainKeyword) == 0 &&
				len(rule.DefaultOptions.DomainRegex) == 0 &&
				len(rule.DefaultOptions.SourceIPCIDR) == 0 &&
				len(rule.DefaultOptions.IPCIDR) == 0 {
				continue
			}
		}
		rules = append(rules, rule)
	}
	plain.Rules = rules
	var buffer bytes.Buffer
	err = srs.Write(&buffer, plain, compat.Version)
	if err != nil {
		return err
	}
	return os.WriteFile(destPath, buffer.Bytes(), 0o644)
}
