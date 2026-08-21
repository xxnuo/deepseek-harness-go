package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

type IndexInjectionKind string

const (
	IndexInjectionGlobal    IndexInjectionKind = "global"
	IndexInjectionScript    IndexInjectionKind = "script"
	IndexInjectionScriptSrc IndexInjectionKind = "script-src"
	IndexInjectionStyle     IndexInjectionKind = "style"
	IndexInjectionHTML      IndexInjectionKind = "html"
)

type IndexInjectionPlacement string

const (
	IndexInjectionHead IndexInjectionPlacement = "head"
	IndexInjectionBody IndexInjectionPlacement = "body"
)

// IndexInjection is the shared, JSON-serializable index boot row used by the
// HTTP renderer and custom frontends embedding the Harness library.
type IndexInjection struct {
	Kind      IndexInjectionKind      `json:"kind"`
	Placement IndexInjectionPlacement `json:"placement,omitempty"`
	Name      string                  `json:"name,omitempty"`
	Value     any                     `json:"value,omitempty"`
	Text      string                  `json:"text,omitempty"`
	Src       string                  `json:"src,omitempty"`
	HTML      string                  `json:"html,omitempty"`
}

// IndexInjectionProvider returns fresh rows for each rendered index.
type IndexInjectionProvider func() ([]IndexInjection, error)

// IndexTransform applies a raw HTML transform after structured rows render.
type IndexTransform func(string) (string, error)

type indexInjectionProviderRegistration struct {
	provide func(*dynamicCordisRun) ([]IndexInjection, error)
}

type indexTapRegistration struct {
	apply func(*dynamicCordisRun, string) (string, error)
}

var (
	indexHeadTag = regexp.MustCompile(`(?i)<head(?:\s[^>]*)?>`)
	indexBodyTag = regexp.MustCompile(`(?i)<body(?:\s[^>]*)?>`)
)

func indexJSON(value any) (string, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return "", err
	}
	return strings.ReplaceAll(strings.TrimSuffix(buffer.String(), "\n"), "<", `\u003c`), nil
}

func indexAttribute(value string) string {
	return strings.NewReplacer("&", "&amp;", `"`, "&quot;", "<", "&lt;", ">", "&gt;").Replace(value)
}

func renderIndexInjection(row IndexInjection) (IndexInjectionPlacement, string, error) {
	switch row.Kind {
	case IndexInjectionGlobal:
		name, err := indexJSON(row.Name)
		if err != nil {
			return "", "", err
		}
		value, err := indexJSON(row.Value)
		if err != nil {
			return "", "", fmt.Errorf("index injection global %q: %w", row.Name, err)
		}
		return IndexInjectionHead, `<script>globalThis[` + name + `] = ` + value + `</script>`, nil
	case IndexInjectionScript:
		if row.Placement != IndexInjectionHead && row.Placement != IndexInjectionBody {
			return "", "", fmt.Errorf("invalid index injection placement %q", row.Placement)
		}
		return row.Placement, `<script>` + row.Text + `</script>`, nil
	case IndexInjectionScriptSrc:
		if row.Placement != IndexInjectionHead && row.Placement != IndexInjectionBody {
			return "", "", fmt.Errorf("invalid index injection placement %q", row.Placement)
		}
		return row.Placement, `<script src="` + indexAttribute(row.Src) + `"></script>`, nil
	case IndexInjectionStyle:
		return IndexInjectionHead, `<style>` + row.Text + `</style>`, nil
	case IndexInjectionHTML:
		if row.Placement != IndexInjectionHead && row.Placement != IndexInjectionBody {
			return "", "", fmt.Errorf("invalid index injection placement %q", row.Placement)
		}
		return row.Placement, row.HTML, nil
	default:
		return "", "", fmt.Errorf("unknown index injection kind %q", row.Kind)
	}
}

// RenderIndexInjections inserts head and body rows in registration order.
func RenderIndexInjections(html string, rows []IndexInjection) (string, error) {
	var head, body strings.Builder
	for _, row := range rows {
		placement, markup, err := renderIndexInjection(row)
		if err != nil {
			return "", err
		}
		if placement == IndexInjectionHead {
			head.WriteString(markup)
		} else {
			body.WriteString(markup)
		}
	}
	if head.Len() > 0 {
		match := indexHeadTag.FindStringIndex(html)
		if match == nil {
			html = head.String() + html
		} else {
			html = html[:match[1]] + head.String() + html[match[1]:]
		}
	}
	if body.Len() > 0 {
		match := indexBodyTag.FindStringIndex(html)
		if match == nil {
			html += body.String()
		} else {
			html = html[:match[1]] + body.String() + html[match[1]:]
		}
	}
	return html, nil
}

func normalizeIndexInjections(rows []IndexInjection) ([]IndexInjection, error) {
	data, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("index injection rows must be JSON values: %w", err)
	}
	var normalized []IndexInjection
	if err := json.Unmarshal(data, &normalized); err != nil {
		return nil, err
	}
	for _, row := range normalized {
		if _, _, err := renderIndexInjection(row); err != nil {
			return nil, err
		}
	}
	return normalized, nil
}

func (e *Engine) registerIndexProvider(provider func(*dynamicCordisRun) ([]IndexInjection, error)) func() {
	e.dynamicCordis.Lock()
	registration := &indexInjectionProviderRegistration{provide: provider}
	e.dynamicCordis.indexProviders = append(e.dynamicCordis.indexProviders, registration)
	e.dynamicCordis.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			e.dynamicCordis.Lock()
			for index, current := range e.dynamicCordis.indexProviders {
				if current == registration {
					e.dynamicCordis.indexProviders = append(e.dynamicCordis.indexProviders[:index], e.dynamicCordis.indexProviders[index+1:]...)
					break
				}
			}
			e.dynamicCordis.Unlock()
		})
	}
}

func (e *Engine) registerIndexTap(transform func(*dynamicCordisRun, string) (string, error)) func() {
	e.dynamicCordis.Lock()
	registration := &indexTapRegistration{apply: transform}
	e.dynamicCordis.indexTaps = append(e.dynamicCordis.indexTaps, registration)
	e.dynamicCordis.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			e.dynamicCordis.Lock()
			for index, current := range e.dynamicCordis.indexTaps {
				if current == registration {
					e.dynamicCordis.indexTaps = append(e.dynamicCordis.indexTaps[:index], e.dynamicCordis.indexTaps[index+1:]...)
					break
				}
			}
			e.dynamicCordis.Unlock()
		})
	}
}

// RegisterIndexInjections contributes fresh rows to HTTP and library renders.
func (e *Engine) RegisterIndexInjections(provider IndexInjectionProvider) func() {
	if provider == nil {
		return func() {}
	}
	return e.registerIndexProvider(func(*dynamicCordisRun) ([]IndexInjection, error) { return provider() })
}

// TapIndex registers a raw transform applied after structured injections.
func (e *Engine) TapIndex(transform IndexTransform) func() {
	if transform == nil {
		return func() {}
	}
	return e.registerIndexTap(func(_ *dynamicCordisRun, html string) (string, error) { return transform(html) })
}

func (e *Engine) baseIndexInjections() ([]IndexInjection, error) {
	graph, _, err := e.buildBootGraph()
	if err != nil {
		return nil, err
	}
	const modulesID = "@deepseek-ai/dsh-client-modules"
	queue := `(()=>{
const pendingQueue=[]
window.__ModuleLoader__={
  mode:"queue",
  pendingQueue,
  load(registration){pendingQueue.push(registration)},
  create(options){
    if(this.mode!=="queue")throw new Error("client-modules: window.__ModuleLoader__.create called after module-system boot")
    const index=pendingQueue.findIndex(registration=>registration.id===` + fmt.Sprintf("%q", modulesID) + `)
    const registration=pendingQueue[index]
    if(registration===undefined)throw new Error("client-modules: HTML did not preload ` + modulesID + `/client.js")
    pendingQueue.splice(index,1)
    const exports=registration.factory(specifier=>{
      throw new Error('client-modules: ` + modulesID + `/client.js requested external "'+specifier+'" before the module system existed')
    })
    if(typeof exports!=="object"||exports===null||typeof exports.createClientModuleSystem!=="function"||typeof exports.apply!=="function"){
      throw new Error("client-modules: ` + modulesID + `/client.js did not export the bootstrap module face")
    }
    return exports.createClientModuleSystem(this,{id:registration.id,exports},options)
  }
}
})()`
	rows := []IndexInjection{{Kind: IndexInjectionScript, Placement: IndexInjectionHead, Text: queue}}
	for _, id := range []string{modulesID, "@deepseek-ai/dsh-client-runtime"} {
		for _, entry := range graph.Entries {
			if entry.ID == id {
				rows = append(rows, IndexInjection{Kind: IndexInjectionScriptSrc, Placement: IndexInjectionHead, Src: entry.URL})
				break
			}
		}
	}
	rows = append(rows, IndexInjection{Kind: IndexInjectionGlobal, Name: "__DSH_BOOT__", Value: graph})
	if e.clientPluginActive("@deepseek-ai/dsh-client-ui-theme") {
		e.mu.RLock()
		value, _ := e.resolvedSettingsValueLocked("ui-theme")
		preference, _ := value["preference"].(string)
		e.mu.RUnlock()
		switch preference {
		case "light", "dark", "system":
		default:
			preference = "system"
		}
		encoded, _ := json.Marshal(preference)
		rows = append(rows, IndexInjection{Kind: IndexInjectionScript, Placement: IndexInjectionBody, Text: `(() => {
  const preference = ` + string(encoded) + `
  const systemDark = preference === 'system'
    && typeof matchMedia !== 'undefined'
    && matchMedia('(prefers-color-scheme: dark)').matches
  const dark = preference === 'dark' || systemDark
  document.documentElement.style.colorScheme = dark ? 'dark' : 'light'
  document.body.toggleAttribute('data-ds-dark-theme', dark)
})()`})
	}
	return rows, nil
}

func (e *Engine) collectIndexInjectionsFrom(caller *dynamicCordisRun) ([]IndexInjection, error) {
	rows, err := e.baseIndexInjections()
	if err != nil {
		return nil, err
	}
	e.dynamicCordis.RLock()
	providers := append([]*indexInjectionProviderRegistration(nil), e.dynamicCordis.indexProviders...)
	e.dynamicCordis.RUnlock()
	for _, registration := range providers {
		provided, err := registration.provide(caller)
		if err != nil {
			return nil, err
		}
		normalized, err := normalizeIndexInjections(provided)
		if err != nil {
			return nil, err
		}
		rows = append(rows, normalized...)
	}
	return e.dynamicIndexInjectionRows(caller, rows)
}

// CollectIndexInjections returns current boot rows for the original UI or a
// custom frontend using the Go library.
func (e *Engine) CollectIndexInjections() ([]IndexInjection, error) {
	return e.collectIndexInjectionsFrom(nil)
}

func (e *Engine) applyIndexTapsFrom(caller *dynamicCordisRun, html string) (string, error) {
	e.dynamicCordis.RLock()
	taps := append([]*indexTapRegistration(nil), e.dynamicCordis.indexTaps...)
	e.dynamicCordis.RUnlock()
	var err error
	for _, registration := range taps {
		html, err = registration.apply(caller, html)
		if err != nil {
			return "", err
		}
	}
	return html, nil
}

// ApplyIndexTaps runs registered raw transforms in registration order.
func (e *Engine) ApplyIndexTaps(html string) (string, error) {
	return e.applyIndexTapsFrom(nil, html)
}

func (e *Engine) renderIndexFrom(caller *dynamicCordisRun, html string) (string, error) {
	rows, err := e.collectIndexInjectionsFrom(caller)
	if err != nil {
		return "", err
	}
	html, err = RenderIndexInjections(html, rows)
	if err != nil {
		return "", err
	}
	return e.applyIndexTapsFrom(caller, html)
}

// RenderIndex renders structured rows, then raw taps, using current state.
func (e *Engine) RenderIndex(html string) (string, error) {
	return e.renderIndexFrom(nil, html)
}
