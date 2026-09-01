package harness

import (
	"strings"
	"testing"
)

func TestRenderIndexInjectionsOrderEscapingAndFragments(t *testing.T) {
	rows := []IndexInjection{
		{Kind: IndexInjectionScript, Placement: IndexInjectionHead, Text: "window.__Q__=1"},
		{Kind: IndexInjectionPreload, Src: `/plugins/??a/client.js&rev="preload"`},
		{Kind: IndexInjectionScriptSrc, Placement: IndexInjectionHead, Src: `/plugins/a.js?rev="1"&x=<y>`},
		{Kind: IndexInjectionGlobal, Name: "__DSH_BOOT__", Value: map[string]any{"rev": "</script><b>"}},
		{Kind: IndexInjectionStyle, Text: "body{margin:0}"},
		{Kind: IndexInjectionHTML, Placement: IndexInjectionHead, HTML: `<meta name="probe">`},
		{Kind: IndexInjectionScript, Placement: IndexInjectionBody, Text: `window.__P__="dark"`},
	}
	html, err := RenderIndexInjections("<html><head></head><body>shell</body></html>", rows)
	if err != nil {
		t.Fatal(err)
	}
	parts := []string{
		"<head>",
		"<script>window.__Q__=1</script>",
		`<link rel="preload" as="script" href="/plugins/??a/client.js&amp;rev=&quot;preload&quot;">`,
		`<script src="/plugins/a.js?rev=&quot;1&quot;&amp;x=&lt;y&gt;"></script>`,
		`globalThis["__DSH_BOOT__"] = {"rev":"\u003c/script>\u003cb>"}`,
		"<style>body{margin:0}</style>",
		`<meta name="probe">`,
		"<body>",
		`<script>window.__P__="dark"</script>`,
		indexReadyMarkup,
		"shell",
	}
	previous := -1
	for _, part := range parts {
		at := strings.Index(html, part)
		if at < 0 || at < previous {
			t.Fatalf("injection order failed at %q: %s", part, html)
		}
		previous = at
	}
	fragment, err := RenderIndexInjections("<main>x</main>", []IndexInjection{
		{Kind: IndexInjectionScript, Placement: IndexInjectionHead, Text: "H"},
		{Kind: IndexInjectionScript, Placement: IndexInjectionBody, Text: "B"},
	})
	if err != nil || fragment != "<script>H</script><main>x</main><script>B</script>"+indexReadyMarkup {
		t.Fatalf("fragment = %q, %v", fragment, err)
	}
	empty, err := RenderIndexInjections("<html><body>shell</body></html>", nil)
	if err != nil || empty != "<html><body>"+indexReadyMarkup+"shell</body></html>" {
		t.Fatalf("empty rows = %q, %v", empty, err)
	}
}

func TestIndexInjectionLibraryRegistrationIsFreshOrderedAndDisposable(t *testing.T) {
	e := newIntegrationEngine(t)
	flag := "first"
	disposeRows := e.RegisterIndexInjections(func() ([]IndexInjection, error) {
		return []IndexInjection{{Kind: IndexInjectionScript, Placement: IndexInjectionHead, Text: `window.__FLAG__="` + flag + `"`}}, nil
	})
	disposeTap := e.TapIndex(func(html string) (string, error) {
		return strings.Replace(html, "shell", "tapped:"+flag, 1), nil
	})
	first, err := e.RenderIndex("<head></head><body>shell</body>")
	if err != nil || !strings.Contains(first, `window.__FLAG__="first"`) || !strings.Contains(first, "tapped:first") {
		t.Fatalf("first render = %q, %v", first, err)
	}
	flag = "second"
	second, err := e.RenderIndex("<head></head><body>shell</body>")
	if err != nil || !strings.Contains(second, `window.__FLAG__="second"`) || !strings.Contains(second, "tapped:second") {
		t.Fatalf("second render = %q, %v", second, err)
	}
	disposeTap()
	disposeRows()
	disposed, err := e.RenderIndex("<head></head><body>shell</body>")
	if err != nil || strings.Contains(disposed, "__FLAG__") || !strings.Contains(disposed, ">shell</body>") {
		t.Fatalf("disposed render = %q, %v", disposed, err)
	}
}

func TestDynamicCordisWebServerIndexInjectionAndTapLifecycle(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "dynamic-index", "")
	if err != nil {
		t.Fatal(err)
	}
	pluginID, runID := runDynamicBuiltinPlugin(t, e, sessionID, "wsi", `
let flag = 'first'
return {
  inject: ['webServer'],
  apply(ctx) {
    ctx.on('webserver/index-inject', table => {
      table.push({ kind: 'script', placement: 'head', text: 'window.__DYNAMIC__=' + JSON.stringify(flag) })
    })
    ctx.webServer.tapIndex(html => html.replace('shell', 'tap:' + flag))
    harness.handle('render', html => ctx.webServer.renderIndex(html))
    harness.handle('set', value => { flag = value; return flag })
    harness.handle('collect', () => ctx.webServer.collectIndexInjections())
  }
}`)
	first := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "render", "<head></head><body>shell</body>")
	if !first.OK {
		t.Fatalf("first render = %#v", first)
	}
	firstHTML := first.Value.(string)
	if !strings.Contains(firstHTML, `window.__DYNAMIC__="first"`) || !strings.Contains(firstHTML, "tap:first") {
		t.Fatalf("first dynamic HTML = %q", firstHTML)
	}
	if set := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "set", "second"); !set.OK {
		t.Fatalf("set = %#v", set)
	}
	second := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "render", "<head></head><body>shell</body>")
	if !second.OK {
		t.Fatalf("second render = %#v", second)
	}
	secondHTML := second.Value.(string)
	if !strings.Contains(secondHTML, `window.__DYNAMIC__="second"`) || !strings.Contains(secondHTML, "tap:second") {
		t.Fatalf("second dynamic HTML = %q", secondHTML)
	}
	collected := e.DynamicCordisInvoke(t.Context(), pluginID, runID, "collect", nil)
	if !collected.OK || len(collected.Value.([]any)) < 2 {
		t.Fatalf("collected rows = %#v", collected)
	}
	if stopped, err := e.DynamicCordisStop(sessionID, pluginID); err != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, err)
	}
	after, err := e.RenderIndex("<head></head><body>shell</body>")
	if err != nil || strings.Contains(after, "__DYNAMIC__") || strings.Contains(after, "tap:second") {
		t.Fatalf("render after stop = %q, %v", after, err)
	}
}
