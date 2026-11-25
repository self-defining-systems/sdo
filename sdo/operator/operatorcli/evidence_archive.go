package operatorcli

import (
	"archive/tar"
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"sdo.dev/operator/evidencesink"
)

// RunEvidenceArchive writes the files named on paths (one per line, relative to root) to out as a tar with the known
// credential values redacted. It is the writer stage of the runtime-artifacts archive script, inside the Pod that
// holds the credentials. live accepts a file that disappeared after it was listed.
func RunEvidenceArchive(root string, mounted []string, live bool, paths io.Reader, out io.Writer) error {
	credentials := &evidencesink.Credentials{}
	if err := credentials.Load(mounted, root); err != nil {
		return err
	}
	archive := tar.NewWriter(out)
	names := bufio.NewScanner(paths)
	for names.Scan() {
		err := archiveRedacted(archive, credentials, root, names.Text())
		if live && errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("archive %s: %w", names.Text(), err)
		}
	}
	if err := names.Err(); err != nil {
		return err
	}
	return archive.Close()
}

func archiveRedacted(archive *tar.Writer, credentials *evidencesink.Credentials, root, name string) error {
	file, err := os.OpenFile(filepath.Join(root, name), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	// A tar entry states its size first, so the first read counts and the second writes; both stop at the size the
	// file had when opened, so a session still appending is archived as a consistent prefix.
	content := func() io.Reader { return credentials.Reader(io.NewSectionReader(file, 0, info.Size())) }
	size, err := io.Copy(io.Discard, content())
	if err != nil {
		return err
	}
	if err := archive.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name, Size: size, Mode: 0o600, ModTime: info.ModTime()}); err != nil {
		return err
	}
	_, err = io.Copy(archive, content())
	return err
}

// RunEvidenceArchiveProcess is the process entry point: `sdo-operator evidence-archive [live]` in the runtime root.
func RunEvidenceArchiveProcess(args []string) error {
	if len(args) > 1 || len(args) == 1 && args[0] != "live" {
		return errors.New("usage: sdo-operator evidence-archive [live]")
	}
	return RunEvidenceArchive(".", evidencesink.MountedCredentialRoots(), len(args) == 1, os.Stdin, os.Stdout)
}
