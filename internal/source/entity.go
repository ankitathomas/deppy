package source

import (
	"fmt"
)

type EntityID struct {
	CSVName string `json:"name,omitempty"`
	Package string `json:"package,omitempty"`
	Version string `json:"version,omitempty"`
	Source string`json:"source,omitempty"` //?
}

type EntityPropertyNotFoundError string

func (p EntityPropertyNotFoundError) Error() string {
	return fmt.Sprintf("Property '(%s)' Not Found", string(p))
}

type Entity struct {
	Id         EntityID `json:"id,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

func NewEntity(id EntityID, properties map[string]string) *Entity {
	return &Entity{
		Id:         id,
		Properties: properties,
	}
}

func (e *Entity) ID() EntityID {
	return e.Id
}

func (e *Entity) GetProperty(key string) (string, error) {
	value, ok := e.Properties[key]
	if !ok {
		return "", EntityPropertyNotFoundError(key)
	}
	return value, nil
}
