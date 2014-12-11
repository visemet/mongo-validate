package validate

import (
	"encoding/json"
	"fmt"
	"gopkg.in/mgo.v2"
	"strings"
)

type anyMap map[interface{}]interface{}

type DocStore struct {
	fields []string
	data   anyMap
}

func coerceHashable(value interface{}) interface{} {
	switch value.(type) {
	case []interface{}, Document: // handle unhashable types
		return fmt.Sprintf("%v", value)
	}
	return value
}

// Appends the specified document to the array (or creates one)
// corresponding to its fields that are indexed
func (kds DocStore) Put(doc Document) error {
	diskLoc, ok := doc.DiskLoc()
	if !ok {
		data, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			return err
		}
		return fmt.Errorf("Document %v is missing a DiskLoc\n", string(data))
	}

	curr := kds.data

	for i := 0; i < len(kds.fields)-1; i++ { // skips last indexed field
		field := kds.fields[i]
		value := coerceHashable(doc[field])

		if _, ok := curr[value]; !ok {
			curr[value] = anyMap{}
		}
		curr = curr[value].(anyMap)
	}

	lastField := kds.fields[len(kds.fields)-1]
	lastValue := coerceHashable(doc[lastField])

	// Use an array to handle non-unique indexes
	if _, ok := curr[lastValue]; !ok {
		curr[lastValue] = []DiskLoc{}
	}
	curr[lastValue] = append(curr[lastValue].([]DiskLoc), diskLoc)

	return nil
}

// Returns the set of documents with fields matching the specified document
func (kds DocStore) get(doc Document) ([]DiskLoc, bool) {
	curr := kds.data

	for i := 0; i < len(kds.fields)-1; i++ { // skips last indexed field
		field := kds.fields[i]
		value := coerceHashable(doc[field])

		if _, ok := curr[value]; !ok {
			return nil, false
		}
		curr = curr[value].(anyMap)
	}

	lastField := kds.fields[len(kds.fields)-1]
	lastValue := coerceHashable(doc[lastField])

	matchedDocs, ok := curr[lastValue]
	if !ok {
		return nil, false
	}
	return matchedDocs.([]DiskLoc), true
}

func (kds DocStore) Contains(doc Document) (bool, error) {
	diskLoc, ok := doc.DiskLoc()
	if !ok {
		data, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("Document %v is missing a DiskLoc\n", string(data))
	}

	matched, ok := kds.get(doc)
	if !ok {
		return false, nil
	}

	for _, other := range matched {
		if diskLoc == other {
			return true, nil
		}
	}
	return false, nil
}

func NewDocStore(index mgo.Index) DocStore {
	keys := []string{}
	for _, key := range index.Key {
		field := key
		if strings.HasPrefix(key, "-") {
			field = field[1:]
		}
		keys = append(keys, field)
	}

	return DocStore{fields: keys, data: anyMap{}}
}
