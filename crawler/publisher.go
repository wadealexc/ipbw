package crawler

// Publisher publishes reports to a Server after some interval
type Publisher struct {
	*Client
	*Reporter

	PublishInterval uint
	Server          string
}
