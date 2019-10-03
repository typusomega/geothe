// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/typusomega/goethe/pkg/api (interfaces: Consumer)

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	spec "github.com/typusomega/goethe/pkg/spec"
)

// MockConsumer is a mock of Consumer interface
type MockConsumer struct {
	ctrl     *gomock.Controller
	recorder *MockConsumerMockRecorder
}

// MockConsumerMockRecorder is the mock recorder for MockConsumer
type MockConsumerMockRecorder struct {
	mock *MockConsumer
}

// NewMockConsumer creates a new mock instance
func NewMockConsumer(ctrl *gomock.Controller) *MockConsumer {
	mock := &MockConsumer{ctrl: ctrl}
	mock.recorder = &MockConsumerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockConsumer) EXPECT() *MockConsumerMockRecorder {
	return m.recorder
}

// Stream mocks base method
func (m *MockConsumer) Stream(arg0 *spec.Cursor) (*spec.Cursor, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stream", arg0)
	ret0, _ := ret[0].(*spec.Cursor)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Stream indicates an expected call of Stream
func (mr *MockConsumerMockRecorder) Stream(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stream", reflect.TypeOf((*MockConsumer)(nil).Stream), arg0)
}