package testeval

func NewResult(sample *Sample, testOutput, evalOutput string) *Result {
	return &Result{
		sample:     sample,
		testOutput: testOutput,
		evalOutput: evalOutput,
	}
}

type Result struct {
	sample     *Sample
	testOutput string
	evalOutput string
}

func (r *Result) SetTestOutput(testOutput string) {
	r.testOutput = testOutput
}

func (r *Result) SetEvalOutput(evalOutput string) {
	r.evalOutput = evalOutput
}

func (r *Result) Sample() *Sample {
	return r.sample
}

func (r *Result) SampleId() int {
	return r.sample.Id()
}

func (r *Result) TestInput() string {
	return r.sample.TestInput()
}

func (r *Result) EvalInput() string {
	return r.sample.EvalInput()
}

func (r *Result) TestOutput() string {
	return r.testOutput
}

func (r *Result) EvalOutput() string {
	return r.evalOutput
}
