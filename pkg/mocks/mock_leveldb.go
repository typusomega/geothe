// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/typusomega/goethe/pkg/storage (interfaces: LevelDB)

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	leveldb "github.com/syndtr/goleveldb/leveldb"
	iterator "github.com/syndtr/goleveldb/leveldb/iterator"
	opt "github.com/syndtr/goleveldb/leveldb/opt"
	util "github.com/syndtr/goleveldb/leveldb/util"
)

// MockLevelDB is a mock of LevelDB interface
type MockLevelDB struct {
	ctrl     *gomock.Controller
	recorder *MockLevelDBMockRecorder
}

// MockLevelDBMockRecorder is the mock recorder for MockLevelDB
type MockLevelDBMockRecorder struct {
	mock *MockLevelDB
}

// NewMockLevelDB creates a new mock instance
func NewMockLevelDB(ctrl *gomock.Controller) *MockLevelDB {
	mock := &MockLevelDB{ctrl: ctrl}
	mock.recorder = &MockLevelDBMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockLevelDB) EXPECT() *MockLevelDBMockRecorder {
	return m.recorder
}

// Close mocks base method
func (m *MockLevelDB) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close
func (mr *MockLevelDBMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockLevelDB)(nil).Close))
}

// Get mocks base method
func (m *MockLevelDB) Get(arg0 []byte, arg1 *opt.ReadOptions) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", arg0, arg1)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Get indicates an expected call of Get
func (mr *MockLevelDBMockRecorder) Get(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockLevelDB)(nil).Get), arg0, arg1)
}

// NewIterator mocks base method
func (m *MockLevelDB) NewIterator(arg0 *util.Range, arg1 *opt.ReadOptions) iterator.Iterator {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewIterator", arg0, arg1)
	ret0, _ := ret[0].(iterator.Iterator)
	return ret0
}

// NewIterator indicates an expected call of NewIterator
func (mr *MockLevelDBMockRecorder) NewIterator(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewIterator", reflect.TypeOf((*MockLevelDB)(nil).NewIterator), arg0, arg1)
}

// Write mocks base method
func (m *MockLevelDB) Write(arg0 *leveldb.Batch, arg1 *opt.WriteOptions) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Write indicates an expected call of Write
func (mr *MockLevelDBMockRecorder) Write(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockLevelDB)(nil).Write), arg0, arg1)
}
