package runtime

// submitCmd is sent to a per-attempt goroutine to initiate job creation.
type submitCmd struct {
	req     AttemptRequest
	replyCh chan<- submitResult
}

// submitResult is the reply from the per-attempt goroutine after JobClient.Create.
type submitResult struct {
	handle AttemptHandle
	err    error
}
