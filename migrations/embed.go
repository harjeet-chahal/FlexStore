// Package migrations embeds the FlexStore control-plane SQL migrations.
//
// They live at the repository root (rather than under internal/) because they
// are a first-class artifact: reviewers, DBAs and `make migrate` all want to
// find them without spelunking through Go packages. Embedding them into the
// coordinator binary means the image can never drift from the schema it was
// built against.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
