//go:build robustness

package compat

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"go.etcd.io/etcd/tests/v3/robustness/model"
)

func normalizeRecordedRequest(request *model.EtcdRequest) {
	if request.Txn == nil {
		return
	}
	// The v3.6.5 recorder hashes range values but keeps raw values in Txn Put inputs.
	// Use its value encoding for both sides. The upstream checker stays unchanged.
	for _, branch := range [][]model.EtcdOperation{
		request.Txn.OperationsOnSuccess, request.Txn.OperationsOnFailure,
	} {
		for i := range branch {
			if branch[i].Type == model.PutOperation && branch[i].Put.Value.Hash == 0 {
				branch[i].Put.Value = model.ToValueOrHash(branch[i].Put.Value.Value)
			}
		}
	}
}

func TestRobustnessReplay(t *testing.T) {
	path := os.Getenv("SLATEDB_REPLAY_HISTORY")
	if path == "" {
		t.Skip("set SLATEDB_REPLAY_HISTORY to operations.json")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var saved []struct {
		ClientId     int
		Input        model.EtcdRequest
		Output       model.MaybeEtcdResponse
		Call, Return int64
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved) == 0 {
		t.Fatal("empty history")
	}
	operations := make([]porcupine.Operation, len(saved))
	for i, op := range saved {
		normalizeRecordedRequest(&op.Input)
		operations[i] = porcupine.Operation{ClientId: op.ClientId, Input: op.Input,
			Output: op.Output, Call: op.Call, Return: op.Return}
	}
	timeout := 30 * time.Second
	if value := os.Getenv("SLATEDB_CHECK_TIMEOUT"); value != "" {
		var err error
		timeout, err = time.ParseDuration(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	result := porcupine.CheckOperationsTimeout(model.NonDeterministicModel, operations, timeout)
	if result != porcupine.Ok {
		t.Fatalf("history check: %s", result)
	}
}

func TestRobustnessRecorderLongTransactionValue(t *testing.T) {
	value := "a transaction value longer than twenty bytes"
	request := model.EtcdRequest{Type: model.Txn, Txn: &model.TxnRequest{
		OperationsOnSuccess: []model.EtcdOperation{{Type: model.PutOperation,
			Put: model.PutOptions{Key: "key", Value: model.ValueOrHash{Value: value}}}}}}

	putOK := model.MaybeEtcdResponse{EtcdResponse: model.EtcdResponse{Revision: 2,
		Txn: &model.TxnResponse{Results: []model.EtcdOperationResult{{}}}}}
	read := model.EtcdRequest{Type: model.Range,
		Range: &model.RangeRequest{RangeOptions: model.RangeOptions{Start: "key"}}}
	response := model.MaybeEtcdResponse{EtcdResponse: model.EtcdResponse{Revision: 2,
		Range: &model.RangeResponse{Count: 1, KVs: []model.KeyValue{{Key: "key",
			ValueRevision: model.ValueRevision{Value: model.ToValueOrHash(value),
				ModRevision: 2, Version: 1}}}}}}
	check := func() porcupine.CheckResult {
		return porcupine.CheckOperationsTimeout(model.NonDeterministicModel,
			[]porcupine.Operation{
				{Input: request, Output: putOK, Call: 1, Return: 2},
				{Input: read, Output: response, Call: 3, Return: 4},
			}, time.Second)
	}
	if check() != porcupine.Illegal {
		t.Fatal("control did not expose the recorder mismatch")
	}
	normalizeRecordedRequest(&request)
	if check() != porcupine.Ok {
		t.Fatal("normalized history did not pass the original model")
	}

}

func TestRobustnessCheckerControls(t *testing.T) {
	put := model.EtcdRequest{Type: model.Txn, Txn: &model.TxnRequest{
		OperationsOnSuccess: []model.EtcdOperation{{Type: model.PutOperation,
			Put: model.PutOptions{Key: "key", Value: model.ToValueOrHash("value")}}}}}
	putOK := model.MaybeEtcdResponse{EtcdResponse: model.EtcdResponse{Revision: 2,
		Txn: &model.TxnResponse{Results: []model.EtcdOperationResult{{}}}}}
	get := model.EtcdRequest{Type: model.Range,
		Range: &model.RangeRequest{RangeOptions: model.RangeOptions{Start: "key"}}}
	present := model.MaybeEtcdResponse{EtcdResponse: model.EtcdResponse{Revision: 2,
		Range: &model.RangeResponse{Count: 1, KVs: []model.KeyValue{{Key: "key",
			ValueRevision: model.ValueRevision{Value: model.ToValueOrHash("value"),
				ModRevision: 2, Version: 1}}}}}}
	lost := model.MaybeEtcdResponse{EtcdResponse: model.EtcdResponse{Revision: 2,
		Range: &model.RangeResponse{KVs: []model.KeyValue{}}}}
	for _, tc := range []struct {
		name        string
		write, read model.MaybeEtcdResponse
		want        porcupine.CheckResult
	}{
		{"valid", putOK, present, porcupine.Ok},
		{"lost-acknowledged-write", putOK, lost, porcupine.Illegal},
		{"uncertain-write-can-persist", model.MaybeEtcdResponse{Error: "timeout"}, present, porcupine.Ok},
	} {
		t.Run(tc.name, func(t *testing.T) {
			operations := []porcupine.Operation{
				{ClientId: 0, Input: put, Output: tc.write, Call: 1, Return: 2},
				{ClientId: 1, Input: get, Output: tc.read, Call: 3, Return: 4},
			}
			result := porcupine.CheckOperationsTimeout(model.NonDeterministicModel, operations, time.Second)
			if result != tc.want {
				t.Fatalf("got %s; want %s", result, tc.want)
			}
		})
	}
}
