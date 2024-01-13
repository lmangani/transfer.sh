package storage

import (
    "bufio"
    "context"
    "fmt"
    "io"
    "os"
    "time"

    "github.com/deta/deta-go/deta"
    "github.com/deta/deta-go/service/drive"
)

type DetaStorage struct {
    drive *drive.Drive
}

func NewDetaStorage(projectKey string, driveName string) (*DetaStorage, error) {
    d, err := deta.New()
    if err != nil {
        return nil, err
    }

    dr, err := drive.New(d, driveName)
    if err != nil {
        return nil, err
    }

    return &DetaStorage{drive: dr}, nil
}

func (ds *DetaStorage) Put(ctx context.Context, token string, filename string, reader io.Reader, contentType string, contentLength uint64) error {
    _, err := ds.drive.Put(&drive.PutInput{
        Name:        filepath.Join(token, filename),
        Body:        bufio.NewReader(reader),
        ContentType: contentType,
    })
    return err
}

func (ds *DetaStorage) Get(ctx context.Context, token string, filename string) (io.ReadCloser, uint64, error) {
    f, err := ds.drive.Get(filepath.Join(token, filename))
    if err != nil {
        return nil, 0, err
    }

    // Assuming the content length is not provided by Deta Drive directly
    content, err := ioutil.ReadAll(f)
    if err != nil {
        return nil, 0, err
    }
    contentLength := uint64(len(content))

    return ioutil.NopCloser(bytes.NewReader(content)), contentLength, nil
}

func (ds *DetaStorage) Delete(ctx context.Context, token string, filename string) error {
    _, err := ds.drive.Delete(filepath.Join(token, filename))
    return err
}

func (ds *DetaStorage) Purge(ctx context.Context, days time.Duration) error {
    lr, err := ds.drive.List(1000, "", "")
    if err != nil {
        return err
    }

    now := time.Now()
    for _, name := range lr.Names {
        info, err := ds.drive.Get(name)
        if err != nil {
            return err
        }

        stat, err := info.Stat()
        if err != nil {
            return err
        }

        if stat.ModTime().Before(now.Add(-1 * days)) {
            if _, err = ds.drive.Delete(name); err != nil {
                return err
            }
        }
    }

    return nil
}

func (ds *DetaStorage) IsNotExist(err error) bool {
    return err == drive.ErrNotFound
}

func (ds *DetaStorage) IsRangeSupported() bool {
    return false // Deta Drive may not support range requests
}
