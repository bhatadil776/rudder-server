// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/router/transformer (interfaces: Transformer)

// Package mocks_transformer is a generated GoMock package.
package mocks_transformer

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	transformer "github.com/rudderlabs/rudder-server/router/transformer"
	types "github.com/rudderlabs/rudder-server/router/types"
)

// MockTransformer is a mock of Transformer interface.
type MockTransformer struct {
	ctrl     *gomock.Controller
	recorder *MockTransformerMockRecorder
}

// MockTransformerMockRecorder is the mock recorder for MockTransformer.
type MockTransformerMockRecorder struct {
	mock *MockTransformer
}

// NewMockTransformer creates a new mock instance.
func NewMockTransformer(ctrl *gomock.Controller) *MockTransformer {
	mock := &MockTransformer{ctrl: ctrl}
	mock.recorder = &MockTransformerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTransformer) EXPECT() *MockTransformerMockRecorder {
	return m.recorder
}

// ProxyRequest mocks base method.
func (m *MockTransformer) ProxyRequest(arg0 context.Context, arg1 *transformer.ProxyRequestParams) (int, string, string) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProxyRequest", arg0, arg1)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(string)
	ret2, _ := ret[2].(string)
	return ret0, ret1, ret2
}

// ProxyRequest indicates an expected call of ProxyRequest.
func (mr *MockTransformerMockRecorder) ProxyRequest(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProxyRequest", reflect.TypeOf((*MockTransformer)(nil).ProxyRequest), arg0, arg1)
}

// Transform mocks base method.
func (m *MockTransformer) Transform(arg0 string, arg1 *types.TransformMessageT) []types.DestinationJobT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Transform", arg0, arg1)
	ret0, _ := ret[0].([]types.DestinationJobT)
	return ret0
}

// Transform indicates an expected call of Transform.
func (mr *MockTransformerMockRecorder) Transform(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Transform", reflect.TypeOf((*MockTransformer)(nil).Transform), arg0, arg1)
}
