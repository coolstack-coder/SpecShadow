// Package spec holds the TLA+ specification used by the checker.
package spec

import _ "embed"

//go:embed TraceRaft.tla
var TraceRaft string
