package storage_test

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/joomcode/errorx"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/typusomega/goethe/pkg/mocks"
	"github.com/typusomega/goethe/pkg/spec"
	"github.com/typusomega/goethe/pkg/storage"
	"github.com/typusomega/goethe/pkg/testhelpers"
)

func TestDiskStorage_GetCursorFor(t *testing.T) {
	type args struct {
		cursor *spec.Cursor
	}
	tests := []struct {
		name  string
		given func(db *mocks.MockLevelDB)
		when  args
		then  func(cursor *spec.Cursor, err error)
	}{
		{
			name: "cursor not found",
			given: func(db *mocks.MockLevelDB) {
				db.EXPECT().Get(gomock.Eq(testhelpers.DefaultCursorCursorKey), gomock.Any()).Return(nil, leveldb.ErrNotFound).Times(1)
			},
			when: args{cursor: &testhelpers.DefaultCursor},
			then: func(cursor *spec.Cursor, err error) {
				assert.NotNil(t, err)
				assert.True(t, errorx.HasTrait(err, errorx.NotFound()))
			},
		},
		{
			name: "unknown error",
			given: func(db *mocks.MockLevelDB) {
				db.EXPECT().Get(gomock.Eq(testhelpers.DefaultCursorCursorKey), gomock.Any()).Return(nil, testhelpers.ErrDefault).Times(1)
			},
			when: args{cursor: &testhelpers.DefaultCursor},
			then: func(cursor *spec.Cursor, err error) {
				assert.Equal(t, testhelpers.ErrDefault, err)
			},
		},
		{
			name: "cursor found",
			given: func(db *mocks.MockLevelDB) {
				db.EXPECT().Get(gomock.Eq(testhelpers.DefaultCursorCursorKey), gomock.Any()).Return(testhelpers.MarshalledDefaultCursor(), nil).Times(1)
			},
			when: args{cursor: &testhelpers.DefaultCursor},
			then: func(cursor *spec.Cursor, err error) {
				assert.Nil(t, err)
				testhelpers.AssertCursorEquals(t, &testhelpers.DefaultCursor, cursor)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			dbMock := mocks.NewMockLevelDB(controller)
			if tt.given != nil {
				tt.given(dbMock)
			}

			it := storage.NewCursors(dbMock, storage.NewIDGenerator(), storage.NewKeyGenerator())
			got, err := it.GetCursorFor(&testhelpers.DefaultCursor)
			tt.then(got, err)
		})
	}
}

func TestDiskStorage_SaveCursor(t *testing.T) {
	type args struct {
		cursor *spec.Cursor
	}
	tests := []struct {
		name  string
		given func(db *mocks.MockLevelDB)
		when  args
		then  func(err error)
	}{
		{
			name: "db write failure",
			given: func(db *mocks.MockLevelDB) {
				db.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
					func(batch *leveldb.Batch, wo *opt.WriteOptions) error {
						assert.Equal(t, batch.Len(), 1)
						return testhelpers.ErrDefault
					}).Times(1)
			},
			when: args{cursor: &testhelpers.DefaultCursor},
			then: func(err error) {
				assert.NotNil(t, err)
				assert.True(t, errorx.IsOfType(err, errorx.RejectedOperation))
			},
		},
		{
			name: "save success",
			given: func(db *mocks.MockLevelDB) {
				db.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
					func(batch *leveldb.Batch, wo *opt.WriteOptions) error {
						assert.Equal(t, batch.Len(), 1)
						return nil
					}).Times(1)
			},
			when: args{cursor: &testhelpers.DefaultCursor},
			then: func(err error) {
				assert.Nil(t, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()
			dbMock := mocks.NewMockLevelDB(controller)
			if tt.given != nil {
				tt.given(dbMock)
			}

			it := storage.NewCursors(dbMock, storage.NewIDGenerator(), storage.NewKeyGenerator())
			err := it.SaveCursor(tt.when.cursor)
			tt.then(err)
		})
	}
}
