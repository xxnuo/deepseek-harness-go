package harness

const openInAppPathToken = "{path}"

type openInAppLaunch struct {
	kind        string
	command     string
	args        []string
	env         map[string]string
	windowsHide bool
}

type openInAppLocator struct {
	kind              string
	launch            openInAppLaunch
	iconPath          string
	fsNames           []string
	name              string
	args              []string
	requiresDesktop   bool
	candidates        []string
	root              string
	namePrefix        string
	relativeLauncher  string
	exe               string
	displayNamePrefix string
	desktopID         string
}

type openInAppPlatformSpec struct {
	locators  []openInAppLocator
	desktopID string
}

type openInAppApp struct {
	id        string
	platforms map[string]openInAppPlatformSpec
}

func openInAppSpec(locators ...openInAppLocator) openInAppPlatformSpec {
	return openInAppPlatformSpec{locators: locators}
}

func openInAppDesktopSpec(desktopID string, locators ...openInAppLocator) openInAppPlatformSpec {
	return openInAppPlatformSpec{locators: locators, desktopID: desktopID}
}

func openInAppMacApp(names ...string) openInAppPlatformSpec {
	return openInAppSpec(openInAppLocator{kind: "app", fsNames: names})
}

func openInAppCLI(name string, args ...string) openInAppLocator {
	return openInAppLocator{kind: "cli", name: name, args: args}
}

func openInAppDesktopCLI(name string, args ...string) openInAppLocator {
	return openInAppLocator{kind: "cli", name: name, args: args, requiresDesktop: true}
}

func openInAppFile(candidates []string, args ...string) openInAppLocator {
	return openInAppLocator{kind: "file", candidates: candidates, args: args}
}

func openInAppAppPaths(exe string, args ...string) openInAppLocator {
	return openInAppLocator{kind: "app-paths", exe: exe, args: args}
}

func openInAppInstallRecordLocator(prefix, relativeLauncher string, args ...string) openInAppLocator {
	return openInAppLocator{kind: "install-record", displayNamePrefix: prefix, relativeLauncher: relativeLauncher, args: args}
}

func openInAppJetBrains(id, productName, cliName, windowsExe string, macNames ...string) openInAppApp {
	return openInAppApp{id: id, platforms: map[string]openInAppPlatformSpec{
		"darwin": openInAppMacApp(macNames...),
		"windows": openInAppSpec(
			openInAppLocator{kind: "scan", root: "${ProgramFiles}/JetBrains", namePrefix: productName, relativeLauncher: "bin/" + windowsExe},
			openInAppInstallRecordLocator(productName, "bin/"+windowsExe),
		),
		"linux": openInAppSpec(openInAppCLI(cliName), openInAppFile([]string{"~/.local/share/JetBrains/Toolbox/scripts/" + cliName})),
	}}
}

var openInAppCatalog = []openInAppApp{
	{id: "finder", platforms: map[string]openInAppPlatformSpec{"darwin": openInAppSpec(openInAppLocator{
		kind: "fixed", launch: openInAppLaunch{kind: "shell-open"}, iconPath: "/System/Library/CoreServices/Finder.app",
	})}},
	{id: "explorer", platforms: map[string]openInAppPlatformSpec{"windows": openInAppSpec(openInAppLocator{
		kind: "fixed", launch: openInAppLaunch{kind: "shell-open"}, iconPath: "${SystemRoot}/explorer.exe",
	})}},
	{id: "filemanager", platforms: map[string]openInAppPlatformSpec{"linux": openInAppSpec(openInAppDesktopCLI("xdg-open"))}},
	{id: "cursor", platforms: map[string]openInAppPlatformSpec{
		"darwin":  openInAppMacApp("Cursor.app"),
		"windows": openInAppSpec(openInAppAppPaths("Cursor.exe"), openInAppInstallRecordLocator("Cursor", ""), openInAppFile([]string{"${LOCALAPPDATA}/Programs/cursor/Cursor.exe"})),
		"linux":   openInAppSpec(openInAppCLI("cursor")),
	}},
	{id: "vscode", platforms: map[string]openInAppPlatformSpec{
		"darwin":  openInAppMacApp("Visual Studio Code.app"),
		"windows": openInAppSpec(openInAppAppPaths("Code.exe"), openInAppInstallRecordLocator("Microsoft Visual Studio Code", "Code.exe"), openInAppFile([]string{"${LOCALAPPDATA}/Programs/Microsoft VS Code/Code.exe", "${ProgramFiles}/Microsoft VS Code/Code.exe"})),
		"linux":   openInAppDesktopSpec("code", openInAppCLI("code")),
	}},
	{id: "vscodeinsiders", platforms: map[string]openInAppPlatformSpec{
		"darwin":  openInAppMacApp("Visual Studio Code - Insiders.app"),
		"windows": openInAppSpec(openInAppAppPaths("Code - Insiders.exe"), openInAppInstallRecordLocator("Microsoft Visual Studio Code Insiders", "Code - Insiders.exe"), openInAppFile([]string{"${LOCALAPPDATA}/Programs/Microsoft VS Code Insiders/Code - Insiders.exe"})),
		"linux":   openInAppDesktopSpec("code-insiders", openInAppCLI("code-insiders")),
	}},
	{id: "windsurf", platforms: map[string]openInAppPlatformSpec{
		"darwin":  openInAppMacApp("Windsurf.app"),
		"windows": openInAppSpec(openInAppAppPaths("Windsurf.exe"), openInAppInstallRecordLocator("Windsurf", ""), openInAppFile([]string{"${LOCALAPPDATA}/Programs/Windsurf/Windsurf.exe"})),
		"linux":   openInAppSpec(openInAppCLI("windsurf")),
	}},
	{id: "zed", platforms: map[string]openInAppPlatformSpec{
		"darwin": openInAppMacApp("Zed.app", "Zed Preview.app"),
		"linux":  openInAppDesktopSpec("dev.zed.Zed", openInAppCLI("zed"), openInAppLocator{kind: "desktop", desktopID: "dev.zed.Zed"}),
	}},
	{id: "sublimetext", platforms: map[string]openInAppPlatformSpec{
		"darwin":  openInAppMacApp("Sublime Text.app"),
		"windows": openInAppSpec(openInAppAppPaths("sublime_text.exe"), openInAppInstallRecordLocator("Sublime Text", ""), openInAppFile([]string{"${ProgramFiles}/Sublime Text/sublime_text.exe"})),
		"linux":   openInAppDesktopSpec("sublime_text", openInAppCLI("subl")),
	}},
	{id: "xcode", platforms: map[string]openInAppPlatformSpec{"darwin": openInAppSpec(openInAppLocator{kind: "xcode"})}},
	{id: "androidstudio", platforms: map[string]openInAppPlatformSpec{
		"darwin":  openInAppMacApp("Android Studio.app"),
		"windows": openInAppSpec(openInAppInstallRecordLocator("Android Studio", "bin/studio64.exe"), openInAppFile([]string{"${ProgramFiles}/Android/Android Studio/bin/studio64.exe"})),
		"linux":   openInAppSpec(openInAppCLI("studio"), openInAppFile([]string{"~/.local/share/JetBrains/Toolbox/scripts/studio", "/opt/android-studio/bin/studio.sh"})),
	}},
	openInAppJetBrains("intellij", "IntelliJ IDEA", "idea", "idea64.exe", "IntelliJ IDEA.app", "IntelliJ IDEA Ultimate.app", "IntelliJ IDEA CE.app"),
	openInAppJetBrains("pycharm", "PyCharm", "pycharm", "pycharm64.exe", "PyCharm.app", "PyCharm Professional.app", "PyCharm CE.app", "PyCharm Community.app"),
	openInAppJetBrains("webstorm", "WebStorm", "webstorm", "webstorm64.exe", "WebStorm.app"),
	openInAppJetBrains("phpstorm", "PhpStorm", "phpstorm", "phpstorm64.exe", "PhpStorm.app"),
	openInAppJetBrains("goland", "GoLand", "goland", "goland64.exe", "GoLand.app"),
	openInAppJetBrains("rider", "Rider", "rider", "rider64.exe", "Rider.app", "JetBrains Rider.app"),
	openInAppJetBrains("rustrover", "RustRover", "rustrover", "rustrover64.exe", "RustRover.app"),
	{id: "fork", platforms: map[string]openInAppPlatformSpec{
		"darwin":  openInAppMacApp("Fork.app"),
		"windows": openInAppSpec(openInAppInstallRecordLocator("Fork", ""), openInAppFile([]string{"${LOCALAPPDATA}/Fork/Fork.exe"})),
	}},
	{id: "sourcetree", platforms: map[string]openInAppPlatformSpec{"darwin": openInAppMacApp("Sourcetree.app")}},
	{id: "github", platforms: map[string]openInAppPlatformSpec{
		"darwin":  openInAppMacApp("GitHub Desktop.app"),
		"windows": openInAppSpec(openInAppLocator{kind: "github-desktop", root: "${LOCALAPPDATA}/GitHubDesktop"}),
	}},
	{id: "tower", platforms: map[string]openInAppPlatformSpec{"darwin": openInAppMacApp("Tower.app")}},
	{id: "gitkraken", platforms: map[string]openInAppPlatformSpec{"darwin": openInAppMacApp("GitKraken.app")}},
	{id: "smartgit", platforms: map[string]openInAppPlatformSpec{"darwin": openInAppMacApp("SmartGit.app")}},
	{id: "sublimemerge", platforms: map[string]openInAppPlatformSpec{
		"darwin":  openInAppMacApp("Sublime Merge.app"),
		"windows": openInAppSpec(openInAppAppPaths("sublime_merge.exe"), openInAppInstallRecordLocator("Sublime Merge", ""), openInAppFile([]string{"${ProgramFiles}/Sublime Merge/sublime_merge.exe"})),
		"linux":   openInAppDesktopSpec("sublime_merge", openInAppCLI("smerge")),
	}},
	{id: "ghostty", platforms: map[string]openInAppPlatformSpec{
		"darwin": openInAppMacApp("Ghostty.app"),
		"linux":  openInAppDesktopSpec("com.mitchellh.ghostty", openInAppCLI("ghostty", "--working-directory="+openInAppPathToken), openInAppLocator{kind: "desktop", desktopID: "com.mitchellh.ghostty", args: []string{"--working-directory=" + openInAppPathToken}}),
	}},
	{id: "warp", platforms: map[string]openInAppPlatformSpec{"darwin": openInAppMacApp("Warp.app")}},
	{id: "iterm", platforms: map[string]openInAppPlatformSpec{"darwin": openInAppMacApp("iTerm.app")}},
	{id: "kitty", platforms: map[string]openInAppPlatformSpec{
		"darwin": openInAppMacApp("kitty.app"),
		"linux":  openInAppDesktopSpec("kitty", openInAppCLI("kitty", "--directory"), openInAppLocator{kind: "desktop", desktopID: "kitty", args: []string{"--directory"}}),
	}},
	{id: "terminal", platforms: map[string]openInAppPlatformSpec{"darwin": openInAppSpec(openInAppLocator{
		kind: "fixed", launch: openInAppLaunch{kind: "argv", command: "open", args: []string{"-a", "Terminal"}}, iconPath: "/System/Applications/Utilities/Terminal.app",
	})}},
	{id: "windowsterminal", platforms: map[string]openInAppPlatformSpec{"windows": openInAppSpec(openInAppCLI("wt", "-d"))}},
	{id: "gitbash", platforms: map[string]openInAppPlatformSpec{"windows": openInAppSpec(openInAppInstallRecordLocator("Git version", "git-bash.exe", "--cd="+openInAppPathToken), openInAppFile([]string{"${ProgramFiles}/Git/git-bash.exe"}, "--cd="+openInAppPathToken))}},
	{id: "gnometerminal", platforms: map[string]openInAppPlatformSpec{"linux": openInAppDesktopSpec("org.gnome.Terminal", openInAppCLI("gnome-terminal", "--working-directory="+openInAppPathToken), openInAppLocator{kind: "desktop", desktopID: "org.gnome.Terminal", args: []string{"--working-directory=" + openInAppPathToken}})}},
	{id: "konsole", platforms: map[string]openInAppPlatformSpec{"linux": openInAppDesktopSpec("org.kde.konsole", openInAppCLI("konsole", "--workdir"), openInAppLocator{kind: "desktop", desktopID: "org.kde.konsole", args: []string{"--workdir"}})}},
}

func openInAppCatalogByID(id string) *openInAppApp {
	for index := range openInAppCatalog {
		if openInAppCatalog[index].id == id {
			return &openInAppCatalog[index]
		}
	}
	return nil
}
