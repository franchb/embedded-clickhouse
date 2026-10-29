package embeddedclickhouse

import (
	"errors"
	"testing"
)

// testVersion is a fixed version for tests of URL and path construction, so that their
// expectations do not change when the exported version constants move to new patches.
const testVersion ClickHouseVersion = "25.8.16.34-lts"

func TestResolveAsset_Linux(t *testing.T) {
	t.Parallel()

	tests := []struct {
		arch     string
		wantFile string
	}{
		{archAMD64, "clickhouse-common-static-25.8.16.34-amd64.tgz"},
		{archARM64, "clickhouse-common-static-25.8.16.34-arm64.tgz"},
	}
	for _, tt := range tests {
		t.Run("linux/"+tt.arch, func(t *testing.T) {
			t.Parallel()

			asset, err := resolveAsset(testVersion, "linux", tt.arch)
			if err != nil {
				t.Fatal(err)
			}

			if asset.filename != tt.wantFile {
				t.Errorf("filename = %q, want %q", asset.filename, tt.wantFile)
			}

			if asset.assetType != assetArchive {
				t.Errorf("assetType = %d, want assetArchive", asset.assetType)
			}
		})
	}
}

func TestResolveAsset_Darwin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		arch     string
		wantFile string
	}{
		{archAMD64, assetMacOS},
		{archARM64, assetMacOSAARCH64},
	}
	for _, tt := range tests {
		t.Run("darwin/"+tt.arch, func(t *testing.T) {
			t.Parallel()

			asset, err := resolveAsset(testVersion, "darwin", tt.arch)
			if err != nil {
				t.Fatal(err)
			}

			if asset.filename != tt.wantFile {
				t.Errorf("filename = %q, want %q", asset.filename, tt.wantFile)
			}

			if asset.assetType != assetRawBinary {
				t.Errorf("assetType = %d, want assetRawBinary", asset.assetType)
			}
		})
	}
}

func TestResolveAsset_Unsupported(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		goos string
		arch string
	}{
		{"windows", "windows", archAMD64},
		{"linux/386", "linux", "386"},
		{"darwin/386", "darwin", "386"},
		{"freebsd", "freebsd", archAMD64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := resolveAsset(testVersion, tt.goos, tt.arch)
			if !errors.Is(err, ErrUnsupportedPlatform) {
				t.Errorf("err = %v, want ErrUnsupportedPlatform", err)
			}
		})
	}
}

func TestNumericVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   ClickHouseVersion
		want string
	}{
		{"25.8.16.34-lts", "25.8.16.34"},
		{"26.1.3.52-stable", "26.1.3.52"},
		{"24.1.1.1", "24.1.1.1"},
	}
	for _, tt := range tests {
		t.Run(string(tt.in), func(t *testing.T) {
			t.Parallel()

			got := numericVersion(tt.in)
			if got != tt.want {
				t.Errorf("numericVersion(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestDownloadURL(t *testing.T) {
	t.Parallel()

	asset := platformAsset{filename: "clickhouse-common-static-25.8.16.34-amd64.tgz", assetType: assetArchive}

	got := downloadURL("", testVersion, asset)

	want := "https://github.com/ClickHouse/ClickHouse/releases/download/v25.8.16.34-lts/clickhouse-common-static-25.8.16.34-amd64.tgz"
	if got != want {
		t.Errorf("downloadURL = %q, want %q", got, want)
	}
}

func TestDownloadURL_CustomBase(t *testing.T) {
	t.Parallel()

	asset := platformAsset{filename: assetMacOSAARCH64, assetType: assetRawBinary}

	got := downloadURL("https://mirror.example.com/releases", testVersion, asset)

	want := "https://mirror.example.com/releases/v25.8.16.34-lts/clickhouse-macos-aarch64"
	if got != want {
		t.Errorf("downloadURL = %q, want %q", got, want)
	}
}

func TestSHA512URL(t *testing.T) {
	t.Parallel()

	asset := platformAsset{filename: "clickhouse-common-static-25.8.16.34-amd64.tgz", assetType: assetArchive}

	got := sha512URL("", testVersion, asset)

	want := "https://github.com/ClickHouse/ClickHouse/releases/download/v25.8.16.34-lts/clickhouse-common-static-25.8.16.34-amd64.tgz.sha512"
	if got != want {
		t.Errorf("sha512URL = %q, want %q", got, want)
	}
}

func TestSHA512URL_Darwin(t *testing.T) {
	t.Parallel()

	asset := platformAsset{filename: assetMacOSAARCH64, assetType: assetRawBinary}

	got := sha512URL("", testVersion, asset)

	want := "https://github.com/ClickHouse/ClickHouse/releases/download/v25.8.16.34-lts/clickhouse-macos-aarch64.sha512"
	if got != want {
		t.Errorf("sha512URL = %q, want %q", got, want)
	}
}
