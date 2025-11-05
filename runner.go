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

type Runner interface {
	TaskName() string
	ReadSamples(ctx context.Context) ([]*Sample, error)
	WriteResult(ctx context.Context, result *Result) error
	ReadResults(ctx context.Context) ([]*Result, error)
	RunTest(ctx context.Context, sample *Sample) (*Result, error)
	RunEval(ctx context.Context, result *Result) error
	RunAllTestEval(ctx context.Context) error
	RunAllTestOnly(ctx context.Context) error
	RunAllEvalOnly(ctx context.Context) error
}

func NewBaseRunner(self Runner, taskName, appId, appSecret, tableUrl string) *BaseRunner {
	return &BaseRunner{
		self:      self,
		taskName:  taskName,
		appId:     appId,
		appSecret: appSecret,
		tableUrl:  tableUrl,
	}
}

type BaseRunner struct {
	self      Runner
	taskName  string
	appId     string
	appSecret string
	tableUrl  string
	conn      *larkbase.Connection[ResultRecord]
}

func (r *BaseRunner) ReadSamples(ctx context.Context) ([]*Sample, error) {
	panic("implement me")
}

func (r *BaseRunner) RunTest(ctx context.Context, sample *Sample) (*Result, error) {
	panic("implement me")
}

func (r *BaseRunner) RunEval(ctx context.Context, result *Result) error {
	panic("implement me")
}

func (r *BaseRunner) TaskName() string {
	return r.taskName
}

func (r *BaseRunner) connect(ctx context.Context) error {
	if r.conn == nil {
		var err error
		r.conn, err = larkbase.ConnectWithUrl[ResultRecord](ctx, r.appId, r.appSecret, r.tableUrl)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *BaseRunner) WriteResult(ctx context.Context, result *Result) error {
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

func (r *BaseRunner) ReadResults(ctx context.Context) ([]*Result, error) {
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

func (r *BaseRunner) RunAllTestEval(ctx context.Context) error {
	results, _ := r.self.ReadResults(ctx)
	if len(results) > 0 {
		return fmt.Errorf("taskName \"%s\" exists", r.taskName)
	}
	samples, err := r.self.ReadSamples(ctx)
	if err != nil {
		return err
	}
	for _, sample := range samples {
		var result *Result
		result, err = r.self.RunTest(ctx, sample)
		if err != nil {
			return err
		}
		err = r.self.RunEval(ctx, result)
		if err != nil {
			return err
		}
		err = r.self.WriteResult(ctx, result)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *BaseRunner) RunAllTestOnly(ctx context.Context) error {
	results, _ := r.self.ReadResults(ctx)
	if len(results) > 0 {
		return fmt.Errorf("taskName \"%s\" exists", r.taskName)
	}
	samples, err := r.self.ReadSamples(ctx)
	if err != nil {
		return err
	}
	for _, sample := range samples {
		var result *Result
		result, err = r.self.RunTest(ctx, sample)
		if err != nil {
			return err
		}
		err = r.self.WriteResult(ctx, result)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *BaseRunner) RunAllEvalOnly(ctx context.Context) error {
	results, err := r.self.ReadResults(ctx)
	if err != nil {
		return err
	}
	for _, result := range results {
		err = r.self.RunEval(ctx, result)
		if err != nil {
			return err
		}
		err = r.self.WriteResult(ctx, result)
		if err != nil {
			return err
		}
	}
	return nil
}
