package storage

import (
    "bufio"
    "context"
    "io"
    "path/filepath"
    "time"

    "github.com/deta/deta-go/deta"
    "github.com/deta/deta-go/service/drive"
)

type DetaStorage struct {
    drive *drive.Drive
}

func NewDetaStorage(projectKey string, driveName string) (*DetaStorage, error) {
    d, err := deta.New(deta.WithProjectKey(projectKey))
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

    return f, 0, nil // Content length is not available directly
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

    // now := time.Now()
    for _, name := range lr.Names {
        _, err := ds.drive.Delete(name)
        if err != nil {
            return err
        }
    }

    return nil
}

func (ds *DetaStorage) IsNotExist(err error) bool {
    // Note: Update this based on the actual error returned by Deta Drive if not found
    return false
}

func (ds *DetaStorage) IsRangeSupported() bool {
    return false // Deta Drive may not support range requests
}
