package testeval

import (
	"context"
	"fmt"
	"github.com/lincaiyong/larkbase"
	"strconv"
)

type ResultRecord struct {
	larkbase.Meta

	TaskName   larkbase.TextField   `lark:"task_name"`
	SampleId   larkbase.NumberField `lark:"sample_id"`
	TestInput  larkbase.TextField   `lark:"test_input"`
	EvalInput  larkbase.TextField   `lark:"eval_input"`
	TestOutput larkbase.TextField   `lark:"test_output"`
	EvalOutput larkbase.TextField   `lark:"eval_output"`
}

type ReadSamplesFn func(ctx context.Context) ([]*Sample, error)
type RunTestFn func(ctx context.Context, sample *Sample) (*Result, error)
type RunEvalFn func(ctx context.Context, result *Result) error

func NewRunner(appId, appSecret, tableUrl, taskName string, rs ReadSamplesFn, rt RunTestFn, re RunEvalFn) *Runner {
	return &Runner{
		appId:        appId,
		appSecret:    appSecret,
		tableUrl:     tableUrl,
		taskName:     taskName,
		readSampleFn: rs,
		runTestFn:    rt,
		runEvalFn:    re,
	}
}

type Runner struct {
	appId     string
	appSecret string
	tableUrl  string
	conn      *larkbase.Connection[ResultRecord]

	taskName     string
	readSampleFn ReadSamplesFn
	runTestFn    RunTestFn
	runEvalFn    RunEvalFn
}

func (r *Runner) ReadSamples(ctx context.Context) ([]*Sample, error) {
	return r.readSampleFn(ctx)
}

func (r *Runner) RunTest(ctx context.Context, sample *Sample) (*Result, error) {
	return r.runTestFn(ctx, sample)
}

func (r *Runner) RunEval(ctx context.Context, result *Result) error {
	return r.runEvalFn(ctx, result)
}

func (r *Runner) TaskName() string {
	return r.taskName
}

func (r *Runner) connect(ctx context.Context) error {
	if r.conn == nil {
		var err error
		r.conn, err = larkbase.ConnectWithUrl[ResultRecord](ctx, r.appId, r.appSecret, r.tableUrl)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) WriteResult(ctx context.Context, result *Result) error {
	if err := r.connect(ctx); err != nil {
		return err
	}
	var record ResultRecord
	record.TaskName.SetValue(r.taskName)
	record.SampleId.SetIntValue(result.SampleId())
	record.TestInput.SetValue(result.TestInput())
	record.EvalInput.SetValue(result.EvalInput())
	record.TestOutput.SetValue(result.TestOutput())
	record.EvalOutput.SetValue(result.EvalOutput())
	if err := r.conn.Create(&record); err != nil {
		return err
	}
	return nil
}

func (r *Runner) ReadResults(ctx context.Context) ([]*Result, error) {
	if err := r.connect(ctx); err != nil {
		return nil, err
	}
	var records []*ResultRecord
	err := r.conn.FindAll(&records, larkbase.NewFindOption(r.conn.FilterAnd(
		r.conn.Condition().TaskName.Is(r.taskName),
	)))
	if err != nil {
		return nil, err
	}
	results := make([]*Result, len(records))
	for i, record := range records {
		sampleId, _ := strconv.Atoi(record.SampleId.StringValue())
		results[i] = NewResult(
			NewSample(sampleId, record.TestInput.StringValue(), record.EvalInput.StringValue()),
			record.TestOutput.StringValue(), record.EvalOutput.StringValue())
	}
	return results, nil
}

func (r *Runner) RunAllTestEval(ctx context.Context) error {
	results, _ := r.ReadResults(ctx)
	if len(results) > 0 {
		return fmt.Errorf("taskName \"%s\" exists", r.taskName)
	}
	samples, err := r.ReadSamples(ctx)
	if err != nil {
		return err
	}
	for _, sample := range samples {
		var result *Result
		result, err = r.RunTest(ctx, sample)
		if err != nil {
			return err
		}
		err = r.RunEval(ctx, result)
		if err != nil {
			return err
		}
		err = r.WriteResult(ctx, result)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) RunAllTestOnly(ctx context.Context) error {
	results, _ := r.ReadResults(ctx)
	if len(results) > 0 {
		return fmt.Errorf("taskName \"%s\" exists", r.taskName)
	}
	samples, err := r.ReadSamples(ctx)
	if err != nil {
		return err
	}
	for _, sample := range samples {
		var result *Result
		result, err = r.RunTest(ctx, sample)
		if err != nil {
			return err
		}
		err = r.WriteResult(ctx, result)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) RunAllEvalOnly(ctx context.Context) error {
	results, err := r.ReadResults(ctx)
	if err != nil {
		return err
	}
	for _, result := range results {
		err = r.RunEval(ctx, result)
		if err != nil {
			return err
		}
		err = r.WriteResult(ctx, result)
		if err != nil {
			return err
		}
	}
	return nil
}
