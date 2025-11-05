package testeval

func NewSample(id int, testInput, evalInput string) *Sample {
	return &Sample{
		id:        id,
		testInput: testInput,
		evalInput: evalInput,
	}
}

type Sample struct {
	id        int
	testInput string
	evalInput string
}

func (s *Sample) Id() int {
	return s.id
}

func (s *Sample) TestInput() string {
	return s.testInput
}

func (s *Sample) EvalInput() string {
	return s.evalInput
}
