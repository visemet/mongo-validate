package common

import (
	"gopkg.in/mgo.v2/bson"
)

const DiskLocField = "$diskLoc"

type DiskLoc struct {
	File   int
	Offset int
}

type Document bson.M

func (d Document) DiskLoc() (DiskLoc, bool) {
	diskLoc, ok := d[DiskLocField]
	if !ok {
		return DiskLoc{}, false
	}

	switch v := diskLoc.(type) {
	case Document:
		return DiskLoc{
			File:   v["file"].(int),
			Offset: v["offset"].(int),
		}, true
	case bson.M:
		return DiskLoc{
			File:   v["file"].(int),
			Offset: v["offset"].(int),
		}, true
	}
	return DiskLoc{}, false
}

func (d Document) SetDiskLoc(dl DiskLoc) {
	d[DiskLocField] = bson.M{
		"file":   dl.File,
		"offset": dl.Offset,
	}
}

type Iter interface {
	All() ([]Document, error)
	Next() (Document, bool)
	Err() error
	Close() error
}
