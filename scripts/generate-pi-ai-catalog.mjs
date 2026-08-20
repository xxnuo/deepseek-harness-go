import { readFile, writeFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const packageRoot = resolve(root, 'deepseek-harness/packages/llm/llm-pi-ai/node_modules/@earendil-works/pi-ai')
const outputPath = resolve(root, 'pi_ai_catalog.json')
const supported = new Set([
  'anthropic-messages',
  'azure-openai-responses',
  'bedrock-converse-stream',
  'google-generative-ai',
  'google-vertex',
  'mistral-conversations',
  'openai-codex-responses',
  'openai-completions',
  'openai-responses',
])
const packageJSON = JSON.parse(await readFile(resolve(packageRoot, 'package.json'), 'utf8'))
const manifest = JSON.parse(await readFile(resolve(packageRoot, 'dist/providers/data/.manifest.json'), 'utf8'))
const { builtinProviders } = await import(pathToFileURL(resolve(packageRoot, 'dist/providers/all.js')))

const unsupportedProtocols = new Set()
const providers = builtinProviders().map((provider) => {
  const models = provider.getModels().flatMap((model) => {
    if (!supported.has(model.api)) {
      unsupportedProtocols.add(model.api)
      return []
    }
    return [{
      id: model.id,
      name: model.name,
      api: model.api,
      baseUrl: model.baseUrl,
      headers: model.headers,
      reasoning: model.reasoning,
      input: model.input,
      contextWindow: model.contextWindow,
      maxTokens: model.maxTokens,
      thinkingLevelMap: model.thinkingLevelMap,
      compat: model.compat,
    }]
  })
  return {
    id: provider.id,
    name: provider.name,
    baseUrl: provider.baseUrl,
    models,
  }
})

if (unsupportedProtocols.size > 0) {
  throw new Error(`unsupported pi-ai protocols: ${[...unsupportedProtocols].sort().join(', ')}`)
}

const output = `${JSON.stringify({
  packageVersion: packageJSON.version,
  manifestStructureHash: manifest.structureHash,
  supportedProtocols: [...supported],
  unsupportedProtocols: [...unsupportedProtocols].sort(),
  providers,
}, null, 2)}\n`

if (process.argv.includes('--check')) {
  const current = await readFile(outputPath, 'utf8').catch(() => '')
  if (current !== output) {
    console.error('pi_ai_catalog.json is stale; run make generate-pi-ai-catalog')
    process.exitCode = 1
  }
} else {
  await writeFile(outputPath, output)
}
