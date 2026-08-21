package harness

func dynamicSessionView(session *Session) map[string]any {
	session.mu.Lock()
	header := dynamicSessionHeaderValue(session.Header)
	events := make([]any, len(session.Events))
	for i, event := range session.Events {
		events[i] = dynamicSessionEventValue(event)
	}
	id := session.Header.ID
	firstLiveSeq := session.firstLiveSeq
	seq := len(events)
	session.mu.Unlock()
	return map[string]any{
		"header":       header,
		"id":           id,
		"firstLiveSeq": firstLiveSeq,
		"events":       events,
		"seq":          seq,
	}
}
