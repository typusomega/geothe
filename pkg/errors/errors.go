package errors

import "github.com/joomcode/errorx"

var namespace = errorx.NewNamespace("goethe")

var resourceExhausted = errorx.RegisterTrait("resource_exhausted")

func ResourceExhausted() errorx.Trait {
	return resourceExhausted
}

var NotFound = errorx.NewType(namespace, "not_found", errorx.NotFound())
var ResourceExhaustedError = errorx.NewType(namespace, "resource_exhausted", ResourceExhausted())
