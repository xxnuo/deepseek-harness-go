# DeepSeek Harness Go

这是 [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) 的 Golang 移植版。项目保留上游的插件化模型、Cordis 配置、Web UI、运行时插件资源和主要 HTTP/WebSocket 契约，将宿主引擎、CLI、SDK 入口以及本地运行时改为 Go 实现。

当前版本由 [`upstream.lock`](upstream.lock) 固定上游仓库、commit 和版本号。它是非官方移植，不替代上游项目；上游前端和插件资源更新时必须同步更新锁文件、生成目录、运行时资源和公开 Go facade。

## 功能

- `dsh`：Web、headless、custom profile 和插件管理 CLI。
- `dsh-sdk`：通过 JSON-RPC stdin/stdout 调用 Go Harness。
- `dsh-landlock-run`：Linux Landlock 沙箱启动器；在不支持的平台提供兼容入口。
- 保留上游 Web UI、插件包、agent preset、动态 inspect catalog 和 PI-AI catalog。
- 提供可复用的公开 Go API，适合自定义前端、CLI 或服务集成。

## 使用预编译包

GitHub Actions 会在 `v*` 标签上构建并发布 Linux、macOS、Windows 的 amd64/arm64 归档，同时发布 `checksums.txt`。下载对应平台的归档后，直接运行其中的 `dsh`：

```sh
./dsh web --no-open
./dsh --profile headless "总结当前任务"
```

默认 Web 地址为 `http://127.0.0.1:3080`。使用 `--port 0` 可让系统选择空闲端口，使用 `--host`、`--trusted-host` 调整监听和浏览器信任配置。API 密钥和运行数据沿用上游环境变量与 `DSH_HOME` 约定。

## 从源码构建

要求：Go `1.26.6`、Node.js `22.19+`、pnpm `11.7+` 和 Git。构建会检出 `upstream.lock` 指定的 DeepSeek Harness，并使用上游的官方客户端构建产物生成嵌入式运行时资源。

```sh
git clone https://github.com/xxnuo/deepseek-harness-go.git
cd deepseek-harness-go

make prepare-upstream
pnpm --dir deepseek-harness install --frozen-lockfile
pnpm --dir deepseek-harness run build:official
make verify-upstream

make test
make vet
make build                 # 当前平台
make build-all             # 六个平台
```

`make prepare` 是上述准备步骤的组合。`make verify-upstream` 会检查锁定 commit、运行时资源、上游 inventory、生成 catalog、公开 facade 和关键契约。`make test`、`make vet` 和 Makefile 构建目标会临时注入嵌入式资源；直接运行普通 `go test ./...` 不具备该构建 overlay。构建结果位于 `dist/`，文件名格式为 `命令-版本-系统-架构`。

如果只需要临时运行而不生成 `dist/`，可以使用：

```sh
make prepare
make dev
```

## Go API

公开 API 位于仓库根包，内部实现位于 `internal/harness`。例如启动 JSON-RPC SDK：

```go
package main

import (
	"context"
	"os"

	harness "github.com/xxnuo/deepseek-harness-go"
)

func main() {
	engine, err := harness.New()
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	if err := engine.ServeJSONRPC(context.Background(), os.Stdin, os.Stdout); err != nil {
		panic(err)
	}
}
```

也可以使用 `harness.New` 创建引擎并挂载 `engine.Handler()`，自行提供 HTTP 服务或前端。公开类型和方法由 `scripts/gen_public_facade` 生成，修改内部 API 后运行：

```sh
go run ./scripts/gen_public_facade
make verify-public-facade
```

## 与上游的关系

本移植保留原版 README 所描述的插件架构和 Web UI，而不是重新实现一套精简界面。`deepseek-harness/` 是按锁定 commit 检出的本地上游工作树，默认被 `.gitignore` 忽略；`runtime-assets/` 和嵌入 bundle 是由上游构建结果生成的派生产物。

升级上游时请按以下顺序操作：更新 `upstream.lock`，检出对应 commit，安装并运行上游 `build:official`，同步运行时资源和生成 catalog/inventory/facade，最后执行 `make verify-upstream`、完整 Go 测试、race、vet、standalone smoke 和 clean archive smoke。

## 自动构建

- `.github/workflows/ci.yml`：Pull Request、`main` 推送和手动触发时，执行上游同步检查、Go test/race/vet、standalone smoke，并生成六平台归档构建产物。
- `.github/workflows/release.yml`：推送匹配 `upstream.lock` 版本的 `v*` 标签，或手动输入同名标签时，执行同样的验证并发布 GitHub Release。

所有发布归档都包含 `dsh`、`dsh-sdk`、`dsh-landlock-run`、`LICENSE` 和本 README，并生成 SHA-256 校验文件。

## 许可证

本移植使用 MIT License。上游 DeepSeek Harness 及其第三方依赖的许可证和版权声明仍以各自源文件及上游仓库为准。
