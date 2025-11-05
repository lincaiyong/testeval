package main

import (
	"context"
	"github.com/lincaiyong/log"
	"github.com/lincaiyong/testeval"
	"os"
)

func readSamples(ctx context.Context) ([]*testeval.Sample, error) {
	sample := testeval.NewSample(1, "test input", "eval input")
	samples := []*testeval.Sample{
		sample,
	}
	return samples, nil
}

func runTest(ctx context.Context, sample *testeval.Sample) (*testeval.Result, error) {
	log.InfoLog("run test: %v", sample)
	result := testeval.NewResult(sample, "test output", "eval output")
	return result, nil
}

func runEval(ctx context.Context, result *testeval.Result) error {
	panic("implement me")
}

func main() {
	appId, appSecret := os.Getenv("LARK_APP_ID"), os.Getenv("LARK_APP_SECRET")
	tableUrl := "https://bytedance.larkoffice.com/base/RB31bsA7Pa3f5JsKDlhcoTYdnue?table=tblhCLZI2Td2SSGB&view=vew8snFkYj"
	r := testeval.NewRunner(appId, appSecret, tableUrl, "task-124", readSamples, runTest, runEval)
	ctx := context.Background()
	err := r.RunAllTestOnly(ctx)
	if err != nil {
		log.ErrorLog("fail to run: %v", err)
		return
	}
}
