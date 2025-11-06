package testeval

import "github.com/lincaiyong/larkbase"

type ResultRecord struct {
	larkbase.Meta

	TaskName   larkbase.TextField   `lark:"task_name"`
	SampleId   larkbase.NumberField `lark:"sample_id"`
	TestInput  larkbase.TextField   `lark:"test_input"`
	EvalInput  larkbase.TextField   `lark:"eval_input"`
	TestOutput larkbase.TextField   `lark:"test_output"`
	EvalOutput larkbase.TextField   `lark:"eval_output"`
}

func NewResult(recordId string, sampleId int, testInput, evalInput, testOutput, evalOutput string) *Result {
	return &Result{
		recordId:   recordId,
		sampleId:   sampleId,
		testInput:  testInput,
		evalInput:  evalInput,
		testOutput: testOutput,
		evalOutput: evalOutput,
	}
}

type Result struct {
	recordId   string
	sampleId   int
	testInput  string
	evalInput  string
	testOutput string
	evalOutput string
}

func (r *Result) RecordId() string {
	return r.recordId
}

func (r *Result) SetTestOutput(testOutput string) {
	r.testOutput = testOutput
}

func (r *Result) SetEvalOutput(evalOutput string) {
	r.evalOutput = evalOutput
}

func (r *Result) SampleId() int {
	return r.sampleId
}

func (r *Result) TestInput() string {
	return r.testInput
}

func (r *Result) EvalInput() string {
	return r.evalInput
}

func (r *Result) TestOutput() string {
	return r.testOutput
}

func (r *Result) EvalOutput() string {
	return r.evalOutput
}
