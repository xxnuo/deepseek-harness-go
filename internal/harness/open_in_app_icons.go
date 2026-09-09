package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type openInAppIcon struct {
	bytes       []byte
	contentType string
}

func readOpenInAppIconFile(path string) *openInAppIcon {
	extension := strings.ToLower(filepath.Ext(path))
	contentType := ""
	switch extension {
	case ".png":
		contentType = "image/png"
	case ".svg":
		contentType = "image/svg+xml"
	default:
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	return &openInAppIcon{bytes: data, contentType: contentType}
}

func extractOpenInAppBundleIcon(bundle string, timeout time.Duration, internals openInAppInternals) *openInAppIcon {
	resources := filepath.Join(bundle, "Contents", "Resources")
	iconFile := ""
	plist := openInAppCommandOutput("plutil", []string{"-convert", "json", "-o", "-", filepath.Join(bundle, "Contents", "Info.plist")}, timeout, internals)
	if plist != "" {
		var value struct {
			Icon string `json:"CFBundleIconFile"`
		}
		if json.Unmarshal([]byte(plist), &value) == nil && value.Icon != "" {
			iconFile = value.Icon
			if !strings.HasSuffix(iconFile, ".icns") {
				iconFile += ".icns"
			}
		}
	}
	if iconFile == "" {
		entries, err := os.ReadDir(resources)
		if err != nil {
			return nil
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".icns") {
				iconFile = entry.Name()
				break
			}
		}
	}
	if iconFile == "" {
		return nil
	}
	icns := filepath.Join(resources, iconFile)
	if !openInAppIsFile(icns) {
		return nil
	}
	workDir, err := os.MkdirTemp("", "dsh-open-in-app-")
	if err != nil {
		return nil
	}
	defer os.RemoveAll(workDir)
	out := filepath.Join(workDir, "icon.png")
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if _, err := internals.run(ctx, "sips", []string{"-s", "format", "png", "-Z", "128", icns, "--out", out}, nil); err != nil {
		return nil
	}
	return readOpenInAppIconFile(out)
}

const openInAppExtractIconPS1 = `param([string]$Source, [string]$Target)
$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Drawing
$icon = [System.Drawing.Icon]::ExtractAssociatedIcon($Source)
if ($null -eq $icon) { exit 1 }
$bitmap = $icon.ToBitmap()
$bitmap.Save($Target, [System.Drawing.Imaging.ImageFormat]::Png)
`

func extractOpenInAppExecutableIcon(executable string, timeout time.Duration, internals openInAppInternals) *openInAppIcon {
	workDir, err := os.MkdirTemp("", "dsh-open-in-app-")
	if err != nil {
		return nil
	}
	defer os.RemoveAll(workDir)
	script := filepath.Join(workDir, "extract-icon.ps1")
	out := filepath.Join(workDir, "icon.png")
	if os.WriteFile(script, []byte(openInAppExtractIconPS1), 0o600) != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	args := []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", script, executable, out}
	if _, err := internals.run(ctx, "powershell.exe", args, nil); err != nil {
		return nil
	}
	return readOpenInAppIconFile(out)
}

var openInAppIconSizes = []string{"512x512", "256x256", "128x128", "64x64", "48x48", "32x32"}

func findOpenInAppLinuxThemeIcon(name string, directories []string) *openInAppIcon {
	for _, directory := range directories {
		for _, size := range openInAppIconSizes {
			for _, extension := range []string{".png", ".svg"} {
				if icon := readOpenInAppIconFile(filepath.Join(directory, "icons", "hicolor", size, "apps", name+extension)); icon != nil {
					return icon
				}
			}
		}
		if icon := readOpenInAppIconFile(filepath.Join(directory, "icons", "hicolor", "scalable", "apps", name+".svg")); icon != nil {
			return icon
		}
		for _, extension := range []string{".png", ".svg"} {
			if icon := readOpenInAppIconFile(filepath.Join(directory, "pixmaps", name+extension)); icon != nil {
				return icon
			}
		}
	}
	return nil
}

func extractOpenInAppIcon(app openInAppApp, resolved *openInAppResolvedLaunch, timeout time.Duration, value openInAppInternals) *openInAppIcon {
	internals := completedOpenInAppInternals(value)
	if internals.platform == "linux" {
		spec, ok := app.platforms["linux"]
		if !ok || spec.desktopID == "" {
			return nil
		}
		entry, ok := findOpenInAppDesktopEntry(spec.desktopID, internals)
		if !ok || entry.icon == "" {
			return nil
		}
		if filepath.IsAbs(entry.icon) {
			return readOpenInAppIconFile(entry.icon)
		}
		return findOpenInAppLinuxThemeIcon(entry.icon, openInAppXDGDataDirectories(internals))
	}
	if resolved.icon == nil {
		return nil
	}
	if resolved.icon.kind == "app-bundle" {
		return extractOpenInAppBundleIcon(resolved.icon.path, timeout, internals)
	}
	return extractOpenInAppExecutableIcon(resolved.icon.path, timeout, internals)
}
