package khala

import (
	"os"
	"strconv"
	"strings"
)

func GetEnv(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		i, err := strconv.Atoi(value)
		if err != nil {
			return fallback
		}
		return i
	}
	return fallback
}

func GetBoolEnv(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fallback
		}
		return b
	}
	return fallback
}

// Get the string representation of revID, e.g., "namespace/name-abc123"
// "name" will refer to the workload name, hence will help to get the snapshot name to restore
func ExtractName(fullName string) string {
	slashIdx := strings.Index(fullName, "/")
	dashIdx := strings.LastIndex(fullName, "-")

	var extractedName string
	if slashIdx != -1 && dashIdx != -1 && dashIdx > slashIdx {
		extractedName = fullName[slashIdx+1 : dashIdx]
	} else if slashIdx != -1 {
		extractedName = fullName[slashIdx+1:]
	} else {
		extractedName = fullName
	}

	return extractedName
}
