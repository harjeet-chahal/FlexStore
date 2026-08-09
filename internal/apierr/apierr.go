// Package apierr defines FlexStore's structured client-facing error format and
// the mapping from internal gRPC failures to HTTP status codes.
//
// Clients get a stable machine-readable Code plus the request ID, so a support
// conversation can start from "code=NoSuchKey request=..." rather than from a
// screenshot of a 500 page.
package apierr

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Code is a stable, S3-flavoured error identifier.
type Code string

const (
	CodeNoSuchKey            Code = "NoSuchKey"
	CodeNoSuchUpload         Code = "NoSuchUpload"
	CodeInvalidBucketName    Code = "InvalidBucketName"
	CodeInvalidKey           Code = "InvalidKey"
	CodeInvalidPartNumber    Code = "InvalidPartNumber"
	CodeInvalidRequest       Code = "InvalidRequest"
	CodeEntityTooLarge       Code = "EntityTooLarge"
	CodeCorruptObject        Code = "CorruptObject"
	CodeInsufficientCapacity Code = "InsufficientStorageCapacity"
	CodeInsufficientReplicas Code = "InsufficientDurableReplicas"
	CodeServiceUnavailable   Code = "ServiceUnavailable"
	CodeInternalError        Code = "InternalError"
	CodeRequestTimeout       Code = "RequestTimeout"
	CodeClientClosedRequest  Code = "ClientClosedRequest"
	CodeMethodNotAllowed     Code = "MethodNotAllowed"
	CodeUploadIncomplete     Code = "UploadIncomplete"
	CodeInvalidRange         Code = "InvalidRange"
	CodeAccessDenied         Code = "AccessDenied"
	CodeSlowDown             Code = "SlowDown"
)

// Error is a client-facing failure with an HTTP status attached.
type Error struct {
	Status  int
	Code    Code
	Message string
	// Cause is logged server-side and never serialised to the client, so
	// internal details (DSNs, node addresses) do not leak.
	Cause error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return string(e.Code) + ": " + e.Message + ": " + e.Cause.Error()
	}
	return string(e.Code) + ": " + e.Message
}

func (e *Error) Unwrap() error { return e.Cause }

// New builds an Error.
func New(status int, code Code, msg string) *Error {
	return &Error{Status: status, Code: code, Message: msg}
}

// Wrap builds an Error carrying an internal cause.
func Wrap(status int, code Code, msg string, cause error) *Error {
	return &Error{Status: status, Code: code, Message: msg, Cause: cause}
}

// payload is the JSON body clients receive.
type payload struct {
	Error struct {
		Code      Code   `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id,omitempty"`
		Resource  string `json:"resource,omitempty"`
	} `json:"error"`
}

// Write serialises err as JSON and logs the underlying cause. It is safe to
// call after headers were already sent only in the sense that it will not
// panic; callers on the streaming path must check that first.
func Write(w http.ResponseWriter, r *http.Request, requestID string, err error, log *slog.Logger) {
	e := From(err)

	if e.Status >= 500 && e.Status != http.StatusServiceUnavailable {
		log.Error("request failed",
			slog.String("code", string(e.Code)),
			slog.Int("status", e.Status),
			slog.String("error", e.Error()))
	} else {
		log.Info("request rejected",
			slog.String("code", string(e.Code)),
			slog.Int("status", e.Status),
			slog.String("error", e.Error()))
	}

	var body payload
	body.Error.Code = e.Code
	body.Error.Message = e.Message
	body.Error.RequestID = requestID
	if r != nil {
		body.Error.Resource = r.URL.Path
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Flexstore-Error-Code", string(e.Code))
	w.WriteHeader(e.Status)
	// HEAD responses must not carry a body.
	if r != nil && r.Method == http.MethodHead {
		return
	}
	if encErr := json.NewEncoder(w).Encode(body); encErr != nil {
		log.Warn("failed to write error body", slog.String("error", encErr.Error()))
	}
}

// From normalises any error into an *Error, translating gRPC status codes from
// the coordinator and storage nodes into sensible HTTP semantics.
func From(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}

	st, ok := status.FromError(err)
	if !ok {
		return Wrap(http.StatusInternalServerError, CodeInternalError, "internal error", err)
	}

	switch st.Code() {
	case codes.NotFound:
		return Wrap(http.StatusNotFound, CodeNoSuchKey, st.Message(), err)
	case codes.InvalidArgument:
		return Wrap(http.StatusBadRequest, CodeInvalidRequest, st.Message(), err)
	case codes.AlreadyExists:
		return Wrap(http.StatusConflict, CodeInvalidRequest, st.Message(), err)
	case codes.FailedPrecondition:
		// Storage nodes return FailedPrecondition for checksum mismatches.
		return Wrap(http.StatusUnprocessableEntity, CodeCorruptObject, st.Message(), err)
	case codes.ResourceExhausted:
		return Wrap(http.StatusInsufficientStorage, CodeInsufficientCapacity, st.Message(), err)
	case codes.Unavailable:
		return Wrap(http.StatusServiceUnavailable, CodeServiceUnavailable, st.Message(), err)
	case codes.DeadlineExceeded:
		return Wrap(http.StatusGatewayTimeout, CodeRequestTimeout, st.Message(), err)
	case codes.Canceled:
		// 499 is nginx's convention for "client went away"; it keeps genuine
		// server errors out of the 5xx alerting budget.
		return Wrap(499, CodeClientClosedRequest, st.Message(), err)
	case codes.Unimplemented:
		return Wrap(http.StatusNotImplemented, CodeInternalError, st.Message(), err)
	default:
		return Wrap(http.StatusInternalServerError, CodeInternalError, "internal error", err)
	}
}
