package main

import (
	"collscan"
	"docstore"
	"encoding/json"
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"indexscan"
	"log"
	"strings"
	"time"
)

// Output format of the listDatabases command
type databaseList struct {
	Databases []struct {
		Name       string `bson:"name"`
		SizeOnDisk int64  `bson:"sizeOnDisk"`
		Empty      bool   `bson:"empty"`
	} `bson:"databases"`

	TotalSize int64 `bson:"totalSize"`
	Ok        int   `bson:"ok"`
}

func validateCollectionAgainstIndex(coll *mgo.Collection, index mgo.Index) {
	collScan := collscan.New(coll, index)
	indexScan := indexscan.New(coll, index)

	docStore := docstore.New(index)
	for doc, hadNext := collScan.Next(); hadNext; doc, hadNext = collScan.Next() {
		if err := collScan.Err(); err != nil {
			log.Fatal(err)
		}
		docStore.Put(doc)
	}

	for doc, hadNext := indexScan.Next(); hadNext; doc, hadNext = indexScan.Next() {
		if err := indexScan.Err(); err != nil {
			log.Fatal(err)
		}
		if found, err := docStore.Contains(doc); err != nil {
			log.Fatal(err)
		} else if !found {
			data, err := json.MarshalIndent(doc, "", "  ")
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\nDocument %v found in collection '%v', but not index %v\n",
				string(data), coll.FullName, index.Key)
		}
	}
}

func validateIndexAgainstCollection(coll *mgo.Collection, index mgo.Index) {
	collScan := collscan.New(coll, index)
	indexScan := indexscan.New(coll, index)

	docStore := docstore.New(index)
	for doc, hadNext := indexScan.Next(); hadNext; doc, hadNext = indexScan.Next() {
		if err := indexScan.Err(); err != nil {
			log.Fatal(err)
		}
		docStore.Put(doc)
	}

	for doc, hadNext := collScan.Next(); hadNext; doc, hadNext = collScan.Next() {
		if err := collScan.Err(); err != nil {
			log.Fatal(err)
		}
		if found, err := docStore.Contains(doc); err != nil {
			log.Fatal(err)
		} else if !found {
			data, err := json.MarshalIndent(doc, "", "  ")
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("\nDocument %v found in index %v, but not collection '%v'\n",
				string(data), index.Key, coll.FullName)
		}
	}
}

func isSpecialIndex(index mgo.Index) bool {
	for _, key := range index.Key {
		if strings.HasPrefix(key, "$") {
			return true
		}
	}
	return false
}

func isMultiKeyIndex(coll *mgo.Collection, index mgo.Index) bool {
	iter := coll.Find(bson.M{}).Iter()
	defer func() {
		if err := iter.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	var doc bson.M
	for iter.Next(&doc) {
		for _, key := range index.Key {
			value, ok := doc[key]
			if !ok {
				continue
			}

			switch value.(type) {
			case []interface{}:
				return true
			}
		}
	}
	return false
}

func main() {
	session, err := mgo.DialWithTimeout("127.0.0.1:27017", 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	session.SetSocketTimeout(1 * time.Hour)

	result := databaseList{}
	if err := session.Run(bson.D{{"listDatabases", 1}}, &result); err != nil {
		log.Fatal(err)
	} else if result.Ok == 0 {
		log.Fatal(result)
	}

	for _, dbInfo := range result.Databases {
		if dbInfo.Name == "admin" || dbInfo.Name == "local" {
			continue
		}

		collNames, err := session.DB(dbInfo.Name).CollectionNames()
		if err != nil {
			log.Fatal(err)
		}

		for _, collName := range collNames {
			if strings.HasPrefix(collName, "system.") {
				continue
			}

			coll := session.DB(dbInfo.Name).C(collName)
			fmt.Printf("==== %v ====\n", coll.FullName)

			indexes, err := coll.Indexes()
			if err != nil {
				log.Fatal(err)
			}

			for _, index := range indexes {
				if isSpecialIndex(index) {
					fmt.Printf(" ==> Skipping special index: %v\n", index.Key)
					continue
				} else if isMultiKeyIndex(coll, index) {
					fmt.Printf(" ==> Skipping multikey index: %v\n", index.Key)
					continue
				}

				fmt.Printf(" ==> Validating index %v\n", index.Key)
				validateCollectionAgainstIndex(coll, index)
				validateIndexAgainstCollection(coll, index)
			}
		}
	}
}
