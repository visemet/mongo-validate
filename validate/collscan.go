package validate

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type collScan struct {
	iter *mgo.Iter
	err  error
}

func (cs *collScan) All() ([]Document, error) {
	var docs []Document
	for doc, hadNext := cs.Next(); hadNext; doc, hadNext = cs.Next() {
		if err := cs.Err(); err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

func (cs *collScan) Next() (Document, bool) {
	var data Document
	hadNext := cs.iter.Next(&data)
	return data, hadNext
}

func (cs *collScan) Err() error {
	return cs.err
}

func (cs *collScan) Close() error {
	return cs.iter.Close()
}

func NewCollScan(coll *mgo.Collection, index mgo.Index) Iter {
	cs := collScan{}

	query := bson.M{
		"$query":       bson.M{},
		"$orderby":     bson.M{"$natural": 1},
		"$showDiskLoc": true,
	}
	cs.iter = coll.Find(query).Iter()

	return &cs
}
