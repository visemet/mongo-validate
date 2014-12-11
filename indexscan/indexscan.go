package indexscan

import (
	"common"
	"errors"
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type indexScan struct {
	dataIter    *mgo.Iter
	diskLocIter *mgo.Iter
	err         error
}

func (is *indexScan) All() ([]common.Document, error) {
	var docs []common.Document
	for doc, hadNext := is.Next(); hadNext; doc, hadNext = is.Next() {
		if err := is.Err(); err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

func (is *indexScan) Next() (common.Document, bool) {
	var data common.Document
	var other common.Document

	hadNext := is.dataIter.Next(&data)
	if is.diskLocIter.Next(&other) != hadNext {
		is.err = errors.New("Iterators ended on different documents")
		return common.Document{}, false
	} else if !hadNext {
		return common.Document{}, false
	}

	if diskLoc, ok := other.DiskLoc(); ok {
		data.SetDiskLoc(diskLoc)
	} else {
		is.err = fmt.Errorf("Document is missing %v field", common.DiskLocField)
	}

	return data, true
}

func (is *indexScan) Err() error {
	return is.err
}

func (is *indexScan) Close() error {
	err := is.dataIter.Close()
	if err != nil {
		return err
	}
	return is.diskLocIter.Close()
}

func New(coll *mgo.Collection, index mgo.Index) common.Iter {
	is := indexScan{}

	dataQuery := bson.M{
		"$query":     bson.M{},
		"$hint":      index.Name,
		"$returnKey": true,
	}
	is.dataIter = coll.Find(dataQuery).Iter()

	diskLocQuery := bson.M{
		"$query":       bson.M{},
		"$hint":        index.Name,
		"$showDiskLoc": true,
	}
	is.diskLocIter = coll.Find(diskLocQuery).Iter()

	return &is
}
