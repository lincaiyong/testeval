package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/lincaiyong/larkbase"
	"github.com/lincaiyong/log"
	"github.com/lincaiyong/testeval"
	"github.com/lincaiyong/uniapi/service/monica"
	"os"
	"strconv"
	"strings"
)

var appId, appSecret string

type TestInput struct {
	VulnType string `json:"vuln_type"`
	Context  string `json:"context"`
}

func readSamples(ctx context.Context) ([]*testeval.Sample, error) {
	type Record struct {
		larkbase.Meta `lark:"https://bytedance.larkoffice.com/base/YztxbszcKa23pdsDQZjcAZVonoO?table=tblkge9hk5D9K41K&view=vewvQl4Tlv"`
		Id            larkbase.NumberField       `lark:"id"`
		Input         larkbase.TextField         `lark:"input"`
		Tag           larkbase.SingleSelectField `lark:"正负"`
		VulnType      larkbase.TextField         `lark:"vuln_type"`
	}
	conn, err := larkbase.Connect[Record](ctx, appId, appSecret)
	if err != nil {
		return nil, err
	}
	var records []*Record
	err = conn.FindAll(&records, larkbase.NewFindOption(conn.FilterAnd(
		conn.Condition().Id.IsLess(100),
	)))
	if err != nil {
		return nil, err
	}
	log.InfoLog("found %d samples", len(records))
	samples := make([]*testeval.Sample, 0, len(records))
	for _, record := range records {
		sampleId, _ := strconv.Atoi(record.Id.StringValue())
		input := TestInput{
			VulnType: record.VulnType.StringValue(),
			Context:  record.Input.StringValue(),
		}
		b, _ := json.Marshal(input)
		sample := testeval.NewSample(sampleId, string(b), record.Tag.StringValue())
		samples = append(samples, sample)
	}
	return samples, nil
}

func runTest(ctx context.Context, sample *testeval.Sample) (*testeval.Result, error) {
	log.InfoLog("run test: %d", sample.Id())
	monica.Init(os.Getenv("MONICA_SESSION_ID"))
	var input TestInput
	err := json.Unmarshal([]byte(sample.TestInput()), &input)
	if err != nil {
		return nil, err
	}
	q := fmt.Sprintf(`分析以下调用栈是否存在%s漏洞。

格式要求
------
{
	"vuln": true/false,
	"reason": "..."
}

调用栈
-----
%s`, input.VulnType, input.Context)
	ret, err := monica.ChatCompletion(ctx, monica.ModelGPT41Mini, q, func(s string) {
		fmt.Print(s)
	})
	if err != nil {
		return nil, err
	}
	log.InfoLog("result: %s", ret)
	result := testeval.NewResult(sample, ret, "")
	return result, nil
}

func runEval(ctx context.Context, result *testeval.Result) error {
	ok := result.EvalInput() == "正" && strings.Contains(result.TestOutput(), "\"vuln\": true") ||
		result.EvalInput() == "负" && strings.Contains(result.TestOutput(), "\"vuln\": false")
	if ok {
		result.SetEvalOutput("1")
	} else {
		result.SetEvalOutput("0")
	}
	return nil
}

func main() {
	appId, appSecret = os.Getenv("LARK_APP_ID"), os.Getenv("LARK_APP_SECRET")
	tableUrl := "https://bytedance.larkoffice.com/base/RB31bsA7Pa3f5JsKDlhcoTYdnue?table=tblhCLZI2Td2SSGB&view=vew8snFkYj"
	r := testeval.NewRunner(appId, appSecret, tableUrl, "cg-100-2211", readSamples, runTest, runEval)
	ctx := context.Background()
	err := r.RunAllTestEval(ctx, 10)
	if err != nil {
		log.ErrorLog("fail to run: %v", err)
		return
	}
}
