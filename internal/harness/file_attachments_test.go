package harness

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func TestFileUploadPromptBindingAndProviderProjection(t *testing.T) {
	e := newIntegrationEngine(t)
	id, err := e.CreateSession(t.Context(), e.Config().Workspace, "files", "")
	if err != nil {
		t.Fatal(err)
	}
	value, rpcErr := e.remoteFileUpload(t.Context(), "fileUploads/upload", map[string]json.RawMessage{
		"agentId": json.RawMessage(`"` + id + `"`),
		"request": json.RawMessage(`{"data":"aGVsbG8=","name":"../notes.txt"}`),
	})
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	upload := value.(map[string]any)
	receipt := upload["receiptId"].(string)
	ref := upload["file"].(FileAttachmentRef)
	if ref.Name != "notes.txt" || ref.Bytes != 5 {
		t.Fatalf("file ref = %#v", ref)
	}
	request := PromptRequest{
		RPCID: "request-file-1",
		Content: []PromptContentPart{
			{Type: "text", Text: "read "},
			{Type: "file", ReceiptID: receipt},
		},
	}
	if _, err := e.Run(t.Context(), id, request); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.resolveFileReceipt(id, receipt); ok {
		t.Fatal("receipt was not retired after durable user message")
	}
	session, _ := e.getSession(id)
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	var content []ContentBlock
	for _, event := range events {
		if event.Type != "user/message" || eventSourceKind(event.Data) != "user" {
			continue
		}
		message := nestedMessage(event.Data)
		content = contentBlocks(message["content"])
		break
	}
	if len(content) != 2 || content[1].FileAttachment == nil {
		t.Fatalf("durable file content = %#v", content)
	}
	messages := e.hydrateChatMessagesWithLimit([]ChatMessage{{Role: "user", Blocks: content}}, 0)
	if len(messages) != 1 || strings.Contains(messages[0].Content, "hello") ||
		!strings.Contains(messages[0].Content, `File "notes.txt" (5 bytes, sha256:2cf24dba)`) ||
		!strings.Contains(messages[0].Content, "verbatim read-only copy saved at") {
		t.Fatalf("provider file projection = %#v", messages)
	}
}

func TestFileUploadRejectsForeignReceiptAndCommandPreservesOrder(t *testing.T) {
	e := newIntegrationEngine(t)
	first, err := e.CreateSession(t.Context(), e.Config().Workspace, "first", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.CreateSession(t.Context(), e.Config().Workspace, "second", "")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := e.StoreFile(base64.StdEncoding.EncodeToString([]byte("notes")), "notes.txt")
	if err != nil {
		t.Fatal(err)
	}
	receipt := "receipt-file"
	e.fileUploadMu.Lock()
	e.fileUploads = map[string]map[string]stagedFileUpload{first: {receipt: {ref: ref}}}
	e.fileUploadMu.Unlock()
	if _, err := e.Prompt(t.Context(), second, PromptRequest{RPCID: "foreign", Content: []PromptContentPart{{Type: "file", ReceiptID: receipt}}}); err == nil || !strings.Contains(err.Error(), "not uploaded") {
		t.Fatalf("foreign receipt error = %v", err)
	}
	session, _ := e.getSession(first)
	execution, admitted, err := e.executeCommand(t.Context(), session, "/plan use these", []commandSubmitAttachment{
		{Type: "file", ReceiptID: receipt},
		{Type: "image", MediaType: "image/png", Data: b64(attachmentFixtureBytes(t)["image/png"]), Name: "diagram.png"},
	})
	if err != nil || !admitted || execution.Result.Kind != "success" {
		t.Fatalf("command = %#v admitted=%v err=%v", execution, admitted, err)
	}
	session.mu.Lock()
	pending := append([]*queuedPrompt(nil), session.pending...)
	session.mu.Unlock()
	if len(pending) != 1 || len(pending[0].content) != 3 || pending[0].content[0].Type != "file" || pending[0].content[1].Type != "image" || pending[0].content[2].Type != "text" {
		t.Fatalf("mixed command content = %#v", pending)
	}
}

func TestFileAttachmentJSONRoundTrip(t *testing.T) {
	block := ContentBlock{Type: "file", FileAttachment: &FileAttachmentRef{AttachmentID: "sha256:abc", Name: "a.txt", Bytes: 3}}
	data, err := json.Marshal(block)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"type":"file","attachment":{"attachmentId":"sha256:abc","name":"a.txt","bytes":3}}` {
		t.Fatalf("json = %s", data)
	}
	var decoded ContentBlock
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.FileAttachment == nil || decoded.FileAttachment.Name != "a.txt" || decoded.Attachment != nil {
		t.Fatalf("decoded = %#v", decoded)
	}
}
