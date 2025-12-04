package testeval

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/lincaiyong/larkbase"
	"golang.org/x/sync/errgroup"
	"math"
	"strconv"
	"time"
)

type ReadFn func(ctx context.Context) ([]*Result, error)
type RunFn func(ctx context.Context, result *Result) error

func NewRunner(appId, appSecret, sampleTable, resultTable, taskName string, runTestFn, runEvalFn RunFn) *Runner {
	return &Runner{
		appId:       appId,
		appSecret:   appSecret,
		sampleTable: sampleTable,
		resultTable: resultTable,
		taskName:    taskName,
		runTestFn:   runTestFn,
		runEvalFn:   runEvalFn,
	}
}

type Runner struct {
	appId       string
	appSecret   string
	sampleTable string
	testFields  []string
	evalFields  []string
	resultTable string
	conn        *larkbase.Connection[ResultRecord]

	taskName  string
	runTestFn RunFn
	runEvalFn RunFn
}

func (r *Runner) SetTestFields(s ...string) {
	r.testFields = s
}

func (r *Runner) SetEvalFields(s ...string) {
	r.evalFields = s
}

func (r *Runner) ReadSamples(ctx context.Context) ([]*Result, error) {
	conn, err := larkbase.ConnectAny(ctx, r.appId, r.appSecret, r.sampleTable)
	if err != nil {
		return nil, err
	}
	var records []*larkbase.AnyRecord
	err = conn.FindAll(&records, larkbase.NewViewIdFindOption(conn.ViewId()))
	if err != nil {
		return nil, err
	}
	ret := make([]*Result, 0, len(records))
	for _, record := range records {
		sampleId, _ := strconv.Atoi(record.Data["id"])
		if sampleId == 0 {
			return nil, fmt.Errorf("id field is invalid from sample id: %s", record.Data)
		}
		var testInput, evalInput string
		if len(r.testFields) == 1 {
			testInput = record.Data[r.testFields[0]]
		} else {
			data := make(map[string]string)
			for _, k := range r.testFields {
				if record.Data[k] != "" {
					data[k] = record.Data[k]
				}
			}
			b, _ := json.Marshal(data)
			testInput = string(b)
		}
		if len(r.evalFields) == 1 {
			evalInput = record.Data[r.evalFields[0]]
		} else {
			data := make(map[string]string)
			for _, k := range r.evalFields {
				if record.Data[k] != "" {
					data[k] = record.Data[k]
				}
			}
			b, _ := json.Marshal(data)
			evalInput = string(b)
		}
		result := NewResult(record.RecordId, sampleId, testInput, evalInput, "", "")
		ret = append(ret, result)
	}
	return ret, nil
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

func (r *Runner) connectResultTable(ctx context.Context) error {
	if r.conn == nil {
		var err error
		r.conn, err = larkbase.ConnectUrl[ResultRecord](ctx, r.appId, r.appSecret, r.resultTable)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) WriteResult(ctx context.Context, result *Result, create bool) error {
	if err := r.connectResultTable(ctx); err != nil {
		return err
	}
	var record ResultRecord
	record.TaskName.SetValue(r.taskName)
	record.SampleId.SetIntValue(result.SampleId())
	record.TestInput.SetValue(result.TestInput())
	record.EvalInput.SetValue(result.EvalInput())
	record.TestOutput.SetValue(result.TestOutput())
	if result.testCostSec > 0 {
		record.TestCost.SetIntValue(result.testCostSec)
	}
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
	if err := r.connectResultTable(ctx); err != nil {
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
	if len(r.testFields) == 0 {
		return fmt.Errorf("test input fields is empty")
	}

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
				sample.SetEvalOutput(tmp.EvalOutput())
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
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			var err error
			if doTest {
				runStart := time.Now()
				err = r.RunTest(ctx, task)
				if err != nil {
					return fmt.Errorf("fail to run task %d: %w", task.SampleId(), err)
				}
				task.testCostSec = int(math.Round(time.Since(runStart).Seconds()))
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
