package main

import (
	"context"
	"github.com/lincaiyong/log"
	"github.com/lincaiyong/testeval"
	"os"
)

func NewRunner(taskName, appId, appSecret, tableUrl string) *Runner {
	r := &Runner{}
	r.BaseRunner = testeval.NewBaseRunner(r, taskName, appId, appSecret, tableUrl)
	return r
}

type Runner struct {
	*testeval.BaseRunner
}

func (r *Runner) ReadSamples(ctx context.Context) ([]*testeval.Sample, error) {
	sample := testeval.NewSample(1, "test input", "eval input")
	samples := []*testeval.Sample{
		sample,
	}
	return samples, nil
}

func (r *Runner) RunTest(ctx context.Context, sample *testeval.Sample) (*testeval.Result, error) {
	log.InfoLog("run test: %v", sample)
	result := testeval.NewResult(sample, "test output", "eval output")
	return result, nil
}

func (r *Runner) RunEval(ctx context.Context, result *testeval.Result) error {
	panic("implement me")
}

func main() {
	appId, appSecret := os.Getenv("LARK_APP_ID"), os.Getenv("LARK_APP_SECRET")
	tableUrl := "https://bytedance.larkoffice.com/base/RB31bsA7Pa3f5JsKDlhcoTYdnue?table=tblhCLZI2Td2SSGB&view=vew8snFkYj"
	r := NewRunner("task-123", appId, appSecret, tableUrl)
	ctx := context.Background()
	err := r.RunAllTestOnly(ctx)
	if err != nil {
		log.ErrorLog("fail to run: %v", err)
		return
	}
}
