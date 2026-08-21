import { readFileSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

import { EVENT_API, SERVICE_API, TYPE_API } from '../deepseek-harness/packages/extensions/tool-cordis/src/api-catalog.ts'

const outputPath = resolve(import.meta.dirname, '..', 'internal/harness/dynamic_inspect_catalog.json')
const output = `${JSON.stringify({ services: SERVICE_API, events: EVENT_API, types: TYPE_API })}\n`

if (process.argv.includes('--check')) {
  if (readFileSync(outputPath, 'utf8') !== output) {
    console.error('dynamic_inspect_catalog.json is stale; run make generate-dynamic-inspect-catalog')
    process.exitCode = 1
  }
} else {
  writeFileSync(outputPath, output)
}
