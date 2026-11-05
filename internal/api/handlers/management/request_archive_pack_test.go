package management

import (
	"archive/zip"
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
)

// Matches the Python compactor's ZIP_STORED members and nanosecond comments.
func packArchiveFixture(t *testing.T, paths ...string) {
	t.Helper()
	output, err := os.Create(filepath.Join(filepath.Dir(paths[0]), "requests-test.zip"))
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(output)
	for _, path := range paths {
		info, errInfo := os.Stat(path)
		if errInfo != nil {
			t.Fatal(errInfo)
		}
		body, errRead := os.ReadFile(path)
		if errRead != nil {
			t.Fatal(errRead)
		}
		member, errMember := writer.CreateHeader(&zip.FileHeader{Name: filepath.Base(path), Method: zip.Store, Comment: strconv.FormatInt(info.ModTime().UnixNano(), 10)})
		if errMember != nil {
			t.Fatal(errMember)
		}
		if _, err = member.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = output.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if err = os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPackedArchiveDownloadsAndErrorListing(t *testing.T) {
	t.Setenv("CLI_PROXY_REQUEST_ARCHIVE", "1")
	dir := t.TempDir()
	name := "error-v1-responses-2026-11-05T150000-12345678.log"
	path := filepath.Join(dir, name)
	payload := append([]byte("complete exchange\n"), bytes.Repeat([]byte{0, 255, 128, 10}, 10000)...)
	if err := os.WriteFile(path, payload, 0600); err != nil {
		t.Fatal(err)
	}
	if err := logging.ArchiveRequestLog(path); err != nil {
		t.Fatal(err)
	}
	// A reference captured before compaction still opens after sources are removed.
	entries, err := logging.ListRequestLogFiles(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("list: %v %v", entries, err)
	}
	packArchiveFixture(t, path+".zst")
	packed, err := logging.ListRequestLogFiles(dir)
	if err != nil || len(packed) != 1 || !packed[0].Modified.Equal(entries[0].Modified) {
		t.Fatalf("packed list: %v %v", packed, err)
	}
	h := &Handler{cfg: &config.Config{}, logDir: dir}
	for _, byID := range []bool{true, false} {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodGet, "/download", nil)
		if byID {
			c.Params = gin.Params{{Key: "id", Value: "12345678"}}
			h.GetRequestLogByID(c)
		} else {
			c.Params = gin.Params{{Key: "name", Value: name}}
			h.DownloadRequestErrorLog(c)
		}
		if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), payload) {
			t.Fatalf("download by ID=%v: status %d", byID, rec.Code)
		}
		if rec.Header().Get("Content-Disposition") != "attachment; filename=\""+name+"\"" {
			t.Fatal("changed download filename")
		}
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/errors", nil)
	h.GetRequestErrorLogs(c)
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte(name)) {
		t.Fatalf("packed error missing: %s", rec.Body.String())
	}
}
