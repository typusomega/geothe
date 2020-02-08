package errors

import (
	"github.com/joomcode/errorx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var namespace = errorx.NewNamespace("goethe")

var (
	// ResourceExhaustedTrait expresses that the requested resource is exhausted.
	ResourceExhaustedTrait = errorx.RegisterTrait("resource_exhausted")
	// ResourceExhausted expresses that the requested resource is exhausted.
	ResourceExhausted      = errorx.NewType(namespace, "ResourceExhausted", ResourceExhaustedTrait)

	// InternalTrait expresses internal failure.
	InternalTrait = errorx.RegisterTrait("internal")
	// Internal expresses internal failure.
	Internal      = errorx.NewType(namespace, "Internal", InternalTrait)

	// NotFoundTrait expresses that the requested resource is not available.
	NotFoundTrait = errorx.RegisterTrait("not_found")
	// NotFoundTrait expresses that the requested resource is not available.
	NotFound      = errorx.NewType(namespace, "NotFound", NotFoundTrait)

	// InvalidArgumentTrait expresses the given request is invalid.
	InvalidArgumentTrait = errorx.RegisterTrait("invalid_argument")
	// InvalidArgument expresses the given request is invalid.
	InvalidArgument      = errorx.NewType(namespace, "InvalidArgument", InvalidArgumentTrait)

	// FailedPreconditionTrait expresses that a certain pre condition is not met and therefore the operation failed.
	FailedPreconditionTrait = errorx.RegisterTrait("failed_precondition")
	// FailedPrecondition expresses that a certain pre condition is not met and therefore the operation failed.
	FailedPrecondition      = errorx.NewType(namespace, "FailedPrecondition", FailedPreconditionTrait)
)

// ToGrpcError transforms the given error into a gRPC error.
func ToGrpcError(err error) error {
	switch errorx.TypeSwitch(err, ResourceExhausted, Internal, NotFound) {
	case ResourceExhausted:
		return status.Error(codes.ResourceExhausted, err.Error())
	case Internal:
		return status.Error(codes.Internal, err.Error())
	case NotFound:
		return status.Error(codes.NotFound, err.Error())
	case InvalidArgument:
		return status.Error(codes.InvalidArgument, err.Error())
	case FailedPrecondition:
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Unknown, err.Error())
	}
}
