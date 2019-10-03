// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/typusomega/goethe/pkg/storage (interfaces: EventStorage)

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	spec "github.com/typusomega/goethe/pkg/spec"
)

// MockEventStorage is a mock of EventStorage interface
type MockEventStorage struct {
	ctrl     *gomock.Controller
	recorder *MockEventStorageMockRecorder
}

// MockEventStorageMockRecorder is the mock recorder for MockEventStorage
type MockEventStorageMockRecorder struct {
	mock *MockEventStorage
}

// NewMockEventStorage creates a new mock instance
func NewMockEventStorage(ctrl *gomock.Controller) *MockEventStorage {
	mock := &MockEventStorage{ctrl: ctrl}
	mock.recorder = &MockEventStorageMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockEventStorage) EXPECT() *MockEventStorageMockRecorder {
	return m.recorder
}

// Append mocks base method
func (m *MockEventStorage) Append(arg0 *spec.Event) (*spec.Event, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Append", arg0)
	ret0, _ := ret[0].(*spec.Event)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Append indicates an expected call of Append
func (mr *MockEventStorageMockRecorder) Append(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Append", reflect.TypeOf((*MockEventStorage)(nil).Append), arg0)
}

// Read mocks base method
func (m *MockEventStorage) Read(arg0 *spec.Cursor) (*spec.Cursor, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", arg0)
	ret0, _ := ret[0].(*spec.Cursor)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read
func (mr *MockEventStorageMockRecorder) Read(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockEventStorage)(nil).Read), arg0)
}