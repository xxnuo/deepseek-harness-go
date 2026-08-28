package harness

import (
	"errors"
	"fmt"
	"sort"
	"unicode/utf8"
)

const toolResultPruneMarker = "\n\n[... tool result middle pruned ...]\n\n"

type CompactionConfig struct {
	Disabled              bool
	AutoDisabled          bool
	ThresholdRatio        float64
	RetainRatio           float64
	RetainTokens          int
	SummarizationProvider string
	SummarizationModel    string
	MaxTokens             int
	CompactionRetries     int
	MaxOverflowRetries    int
}

type compactionRuntimeConfig struct {
	CompactionConfig
	useRetainTokens bool
	modelPolicies   []compactionModelPolicy
}

type compactionModelPolicy struct {
	Provider              string
	Model                 string
	ThresholdRatio        float64
	RetainRatio           float64
	RetainTokens          int
	useRetainTokens       bool
	SummarizationProvider string
	SummarizationModel    string
	MaxTokens             int
	CompactionRetries     int
	MaxOverflowRetries    int
}

type ToolResultPruneConfig struct {
	Disabled       bool
	ThresholdChars int
	HeadChars      int
	TailChars      int
}

type SpillConfig struct {
	Disabled       bool
	Root           string
	MaxInlineBytes int
}

type RepeatToolReminderConfig struct {
	Disabled              bool
	Thresholds            []int
	Include               []string
	Exclude               []string
	ArgumentsPreviewChars int
}

func defaultCompactionConfig() CompactionConfig {
	return CompactionConfig{
		ThresholdRatio: 0.8, RetainRatio: 0.16, MaxTokens: 8192,
		CompactionRetries: 1, MaxOverflowRetries: 1,
	}
}

func defaultToolResultPruneConfig() ToolResultPruneConfig {
	return ToolResultPruneConfig{ThresholdChars: 8192, HeadChars: 4096, TailChars: 1024}
}

func defaultSpillConfig() SpillConfig { return SpillConfig{MaxInlineBytes: 50000} }

func defaultRepeatToolReminderConfig() RepeatToolReminderConfig {
	return RepeatToolReminderConfig{Thresholds: []int{3, 5, 8}, ArgumentsPreviewChars: 500}
}

func normalizeCompactionConfig(config CompactionConfig) CompactionConfig {
	if config == (CompactionConfig{}) {
		return defaultCompactionConfig()
	}
	defaults := defaultCompactionConfig()
	if config.ThresholdRatio == 0 {
		config.ThresholdRatio = defaults.ThresholdRatio
	}
	if config.RetainRatio == 0 && config.RetainTokens == 0 {
		config.RetainRatio = defaults.RetainRatio
	}
	if config.MaxTokens == 0 {
		config.MaxTokens = defaults.MaxTokens
	}
	return config
}

func newCompactionRuntimeConfig(config CompactionConfig) compactionRuntimeConfig {
	return compactionRuntimeConfig{CompactionConfig: config, useRetainTokens: config.RetainTokens > 0}
}

func cloneCompactionRuntimeConfig(config compactionRuntimeConfig) compactionRuntimeConfig {
	config.modelPolicies = append([]compactionModelPolicy(nil), config.modelPolicies...)
	return config
}

func compactionPolicyFor(config compactionRuntimeConfig, selection ModelSelection) CompactionConfig {
	for _, policy := range config.modelPolicies {
		if policy.Provider != selection.Provider || policy.Model != selection.Model {
			continue
		}
		config.ThresholdRatio = policy.ThresholdRatio
		config.RetainRatio = policy.RetainRatio
		config.RetainTokens = policy.RetainTokens
		if policy.useRetainTokens {
			config.RetainRatio = 0
		}
		config.SummarizationProvider = policy.SummarizationProvider
		config.SummarizationModel = policy.SummarizationModel
		config.MaxTokens = policy.MaxTokens
		config.CompactionRetries = policy.CompactionRetries
		config.MaxOverflowRetries = policy.MaxOverflowRetries
		break
	}
	return config.CompactionConfig
}

func normalizeToolResultPruneConfig(config ToolResultPruneConfig) ToolResultPruneConfig {
	if config == (ToolResultPruneConfig{}) {
		return defaultToolResultPruneConfig()
	}
	defaults := defaultToolResultPruneConfig()
	if config.ThresholdChars == 0 {
		config.ThresholdChars = defaults.ThresholdChars
	}
	return config
}

func normalizeSpillConfig(config SpillConfig) SpillConfig {
	if config == (SpillConfig{}) {
		return defaultSpillConfig()
	}
	return config
}

func normalizeRepeatToolReminderConfig(config RepeatToolReminderConfig) RepeatToolReminderConfig {
	if !config.Disabled && len(config.Thresholds) == 0 && config.ArgumentsPreviewChars == 0 {
		return defaultRepeatToolReminderConfig()
	}
	config.Thresholds = append([]int(nil), config.Thresholds...)
	config.Include = append([]string(nil), config.Include...)
	config.Exclude = append([]string(nil), config.Exclude...)
	sort.Ints(config.Thresholds)
	if config.ArgumentsPreviewChars == 0 {
		config.ArgumentsPreviewChars = defaultRepeatToolReminderConfig().ArgumentsPreviewChars
	}
	return config
}

func validateRuntimePolicies(config Config) error {
	compact := config.Compaction
	if compact.ThresholdRatio <= 0 || compact.ThresholdRatio > 1 {
		return fmt.Errorf("compaction thresholdRatio must be in (0, 1], got %v", compact.ThresholdRatio)
	}
	if compact.RetainTokens < 0 || compact.RetainRatio < 0 || compact.RetainRatio >= compact.ThresholdRatio {
		return errors.New("compaction retention must be non-negative and remain below the threshold")
	}
	if compact.RetainTokens == 0 && compact.RetainRatio == 0 {
		return errors.New("compaction requires retainTokens or retainRatio")
	}
	if compact.MaxTokens <= 0 || compact.CompactionRetries < 0 || compact.MaxOverflowRetries < 0 {
		return errors.New("compaction maxTokens must be positive and retry counts must be non-negative")
	}
	prune := config.ToolResultPruner
	if prune.ThresholdChars <= 0 || prune.HeadChars < 0 || prune.TailChars < 0 {
		return errors.New("tool result prune budgets must be non-negative and thresholdChars must be positive")
	}
	if prune.HeadChars+utf8.RuneCountInString(toolResultPruneMarker)+prune.TailChars > prune.ThresholdChars {
		return errors.New("tool result prune headChars + marker + tailChars must fit thresholdChars")
	}
	if config.Spill.MaxInlineBytes < 0 {
		return errors.New("spill maxInlineBytes must be non-negative")
	}
	if config.RepeatToolReminder.Disabled {
		return nil
	}
	if config.RepeatToolReminder.ArgumentsPreviewChars < 1 || len(config.RepeatToolReminder.Thresholds) == 0 {
		return errors.New("repeat tool reminder needs thresholds and a positive arguments preview")
	}
	thresholds := append([]int(nil), config.RepeatToolReminder.Thresholds...)
	sort.Ints(thresholds)
	for index, value := range thresholds {
		if value < 2 || index > 0 && value == thresholds[index-1] {
			return errors.New("repeat tool reminder thresholds must be unique integers >= 2")
		}
	}
	return nil
}
