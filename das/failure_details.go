package das

import (
	"regexp"
	"strconv"
)

var shareIndexPattern = regexp.MustCompile(`share_index=(\d+)`)

type failedUnitInfo struct {
	reason      string
	retryable   bool
	sampleIndex *uint32
}

func buildFailedUnitInfo(err error) failedUnitInfo {
	if err == nil {
		return failedUnitInfo{}
	}

	info := failedUnitInfo{
		reason:    err.Error(),
		retryable: classifyRetryable(err),
	}

	if sampleIdx, ok := extractSampleIndex(err); ok {
		info.sampleIndex = &sampleIdx
	}

	return info
}

func extractSampleIndex(err error) (uint32, bool) {
	if err == nil {
		return 0, false
	}

	match := shareIndexPattern.FindStringSubmatch(err.Error())
	if len(match) != 2 {
		return 0, false
	}

	v, parseErr := strconv.ParseUint(match[1], 10, 32)
	if parseErr != nil {
		return 0, false
	}

	return uint32(v), true
}

func cloneSampleIndex(idx *uint32) *uint32 {
	if idx == nil {
		return nil
	}
	copy := *idx
	return &copy
}

func shouldExposeFailureInfo(info failedUnitInfo) bool {
	if info.reason != "" {
		return true
	}
	return info.sampleIndex != nil
}

func mergeFailureInfo(base, override failedUnitInfo) failedUnitInfo {
	if shouldExposeFailureInfo(override) {
		return failedUnitInfo{
			reason:      override.reason,
			retryable:   override.retryable,
			sampleIndex: cloneSampleIndex(override.sampleIndex),
		}
	}

	if shouldExposeFailureInfo(base) {
		return failedUnitInfo{
			reason:      base.reason,
			retryable:   base.retryable,
			sampleIndex: cloneSampleIndex(base.sampleIndex),
		}
	}

	return failedUnitInfo{}
}
