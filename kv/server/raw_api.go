package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, nil
	}
	defer reader.Close()

	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, nil
	} else if val == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}

	return &kvrpcpb.RawGetResponse{
		Value: val,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.GetCf(),
				Key:   req.GetKey(),
				Value: req.GetValue(),
			},
		},
	}

	if err := server.storage.Write(nil, batch); err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, nil
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.GetCf(),
				Key: req.GetKey(),
			},
		},
	}

	if err := server.storage.Write(nil, batch); err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, nil
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawScanResponse{
			Error: err.Error(),
		}, nil
	}
	defer reader.Close()

	kvs := make([]*kvrpcpb.KvPair, 0, req.GetLimit())

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	iter.Seek(req.GetStartKey())
	var i uint32
	for iter.Valid() && i < req.GetLimit() {
		val, err := iter.Item().Value()
		if err != nil {
			return &kvrpcpb.RawScanResponse{
				Error: err.Error(),
			}, nil
		}

		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   iter.Item().Key(),
			Value: val,
		})

		iter.Next()
		i++
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
