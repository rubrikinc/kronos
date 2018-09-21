package kronosutil

import (
	"context"
	"io"

	"github.com/rubrikinc/kronos/kronosutil/log"
)

// CloseWithErrorLog closes the given closer and logs the error.
// There are lint checks which prevent defer close() if close() returns an
// error, this closer is useful in such cases.
func CloseWithErrorLog(ctx context.Context, o io.Closer) {
	if err := o.Close(); err != nil {
		log.Error(ctx, err)
	}
}
