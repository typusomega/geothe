package errors

import (
	"github.com/joomcode/errorx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var namespace = errorx.NewNamespace("goethe")

var (
	ResourceExhaustedTrait = errorx.RegisterTrait("resource_exhausted")
	ResourceExhausted      = errorx.NewType(namespace, "ResourceExhausted", ResourceExhaustedTrait)

	InternalTrait = errorx.RegisterTrait("internal")
	Internal      = errorx.NewType(namespace, "Internal", InternalTrait)

	NotFoundTrait = errorx.RegisterTrait("not_found")
	NotFound      = errorx.NewType(namespace, "NotFound", NotFoundTrait)

	InvalidArgumentTrait = errorx.RegisterTrait("invalid_argument")
	InvalidArgument      = errorx.NewType(namespace, "InvalidArgument", InvalidArgumentTrait)

	FailedPreconditionTrait = errorx.RegisterTrait("failed_precondition")
	FailedPrecondition      = errorx.NewType(namespace, "FailedPrecondition", FailedPreconditionTrait)
)

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
