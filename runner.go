package testeval

import (
	"context"
	"fmt"
	"github.com/lincaiyong/larkbase"
	"golang.org/x/sync/errgroup"
	"strconv"
)

type ReadFn func(ctx context.Context) ([]*Result, error)
type RunFn func(ctx context.Context, result *Result) error

func NewRunner(appId, appSecret, tableUrl, taskName string, readFn ReadFn, runTestFn, runEvalFn RunFn) *Runner {
	return &Runner{
		appId:     appId,
		appSecret: appSecret,
		tableUrl:  tableUrl,
		taskName:  taskName,
		readFn:    readFn,
		runTestFn: runTestFn,
		runEvalFn: runEvalFn,
	}
}

type Runner struct {
	appId     string
	appSecret string
	tableUrl  string
	conn      *larkbase.Connection[ResultRecord]

	taskName  string
	readFn    ReadFn
	runTestFn RunFn
	runEvalFn RunFn
}

func (r *Runner) ReadSamples(ctx context.Context) ([]*Result, error) {
	return r.readFn(ctx)
}

func (r *Runner) RunTest(ctx context.Context, result *Result) error {
	return r.runTestFn(ctx, result)
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

func (r *Runner) WriteResult(ctx context.Context, result *Result, create bool) error {
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
	if create {
		if err := r.conn.Create(&record); err != nil {
			return err
		}
	} else {
		record.RecordId = result.RecordId()
		if err := r.conn.Update(&record); err != nil {
			return err
		}
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
		result := NewResult(record.RecordId, sampleId,
			record.TestInput.StringValue(), record.EvalInput.StringValue(),
			record.TestOutput.StringValue(), record.EvalOutput.StringValue())
		results[i] = result
	}
	return results, nil
}

func (r *Runner) run(ctx context.Context, concurrency int, doTest, doEval bool) error {
	results, err := r.ReadResults(ctx)
	if err != nil {
		return fmt.Errorf("fail to read results: %w", err)
	}
	existsResults := make(map[int]*Result)
	for _, result := range results {
		existsResults[result.SampleId()] = result
	}
	samples, err := r.ReadSamples(ctx)
	if err != nil {
		return fmt.Errorf("fail to read samples: %w", err)
	}
	tasks := make(chan *Result, len(samples))
	for _, sample := range samples {
		if doTest {
			if _, ok := existsResults[sample.SampleId()]; !ok {
				tasks <- sample
			}
		} else {
			if tmp, ok := existsResults[sample.SampleId()]; ok {
				sample.SetTestOutput(tmp.TestOutput())
				sample.SetRecordId(tmp.RecordId())
				tasks <- sample
			}
		}
	}
	close(tasks)
	concurrency = min(concurrency, len(samples))
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for task := range tasks {
		task := task
		g.Go(func() error {
			var err error
			if doTest {
				err = r.RunTest(ctx, task)
				if err != nil {
					return fmt.Errorf("fail to run task %d: %w", task.SampleId(), err)
				}
			}
			if doEval {
				err = r.RunEval(ctx, task)
				if err != nil {
					return fmt.Errorf("fail to eval task %d: %w", task.SampleId(), err)
				}
			}
			create := true
			if !doTest {
				create = false
			}
			err = r.WriteResult(ctx, task, create)
			if err != nil {
				return fmt.Errorf("fail to write result for task %d: %w", task.SampleId(), err)
			}
			return nil
		})
	}
	return g.Wait()
}

func (r *Runner) RunAllTestEval(ctx context.Context, concurrency int) error {
	return r.run(ctx, concurrency, true, true)
}

func (r *Runner) RunAllTestOnly(ctx context.Context, concurrency int) error {
	return r.run(ctx, concurrency, true, false)
}

func (r *Runner) RunAllEvalOnly(ctx context.Context, concurrency int) error {
	return r.run(ctx, concurrency, false, true)
}
