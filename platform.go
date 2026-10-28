package embeddedclickhouse

import (
	"fmt"
	"runtime"
	"strings"
)

const defaultBaseURL = "https://github.com/ClickHouse/ClickHouse/releases/download"

const (
	archAMD64 = "amd64"
	archARM64 = "arm64"

	assetMacOS        = "clickhouse-macos"
	assetMacOSAARCH64 = "clickhouse-macos-aarch64"
)

type assetType int

const (
	assetArchive   assetType = iota // .tgz archive (Linux)
	assetRawBinary                  // raw executable (macOS)
)

type platformAsset struct {
	filename  string
	assetType assetType
}

// numericVersion strips the -stable/-lts/-testing suffix from a version string.
// e.g., "25.8.16.34-lts" -> "25.8.16.34".
func numericVersion(v ClickHouseVersion) string {
	s := string(v)
	if i := strings.LastIndex(s, "-"); i != -1 {
		suffix := s[i+1:]
		if suffix == "lts" || suffix == "stable" || suffix == "testing" {
			return s[:i]
		}
	}

	return s
}

func resolveAsset(version ClickHouseVersion, goos, goarch string) (platformAsset, error) {
	switch goos {
	case "linux":
		numVer := numericVersion(version)

		arch, err := linuxArch(goarch)
		if err != nil {
			return platformAsset{}, err
		}

		return platformAsset{
			filename:  fmt.Sprintf("clickhouse-common-static-%s-%s.tgz", numVer, arch),
			assetType: assetArchive,
		}, nil
	case "darwin":
		name, err := darwinAssetName(goarch)
		if err != nil {
			return platformAsset{}, err
		}

		return platformAsset{
			filename:  name,
			assetType: assetRawBinary,
		}, nil
	default:
		return platformAsset{}, fmt.Errorf("%w: %s/%s", ErrUnsupportedPlatform, goos, goarch)
	}
}

func linuxArch(goarch string) (string, error) {
	switch goarch {
	case archAMD64:
		return archAMD64, nil
	case archARM64:
		return archARM64, nil
	default:
		return "", fmt.Errorf("%w: linux/%s", ErrUnsupportedPlatform, goarch)
	}
}

func darwinAssetName(goarch string) (string, error) {
	switch goarch {
	case archAMD64:
		return assetMacOS, nil
	case archARM64:
		return assetMacOSAARCH64, nil
	default:
		return "", fmt.Errorf("%w: darwin/%s", ErrUnsupportedPlatform, goarch)
	}
}

func downloadURL(baseURL string, version ClickHouseVersion, asset platformAsset) string {
	if version == "" {
		return ""
	}

	if baseURL == "" {
		baseURL = defaultBaseURL
	}

	return fmt.Sprintf("%s/v%s/%s", baseURL, string(version), asset.filename)
}

func sha512URL(baseURL string, version ClickHouseVersion, asset platformAsset) string {
	u := downloadURL(baseURL, version, asset)
	if u == "" {
		return ""
	}

	return u + ".sha512"
}

func resolveCurrentPlatformAsset(version ClickHouseVersion) (platformAsset, error) {
	return resolveAsset(version, runtime.GOOS, runtime.GOARCH)
}
