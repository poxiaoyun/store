package controller

import (
	"bytes"
	"fmt"
	"io"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kubeyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func SplitYAMLPartialObject(data []byte) ([]*metav1.PartialObjectMetadata, error) {
	d := kubeyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var objs []*metav1.PartialObjectMetadata
	for {
		u := &metav1.PartialObjectMetadata{}
		if err := d.Decode(u); err != nil {
			if err == io.EOF {
				break
			}
			return objs, fmt.Errorf("failed to unmarshal manifest: %v", err)
		}
		if u.APIVersion == "" && u.Kind == "" {
			continue
		}
		objs = append(objs, u)
	}
	return objs, nil
}

const ReadCache = 4096

func SplitYAML(data []byte) ([]unstructured.Unstructured, error) {
	d := kubeyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), ReadCache)
	var objs []unstructured.Unstructured
	for {
		u := &unstructured.Unstructured{}
		if err := d.Decode(u); err != nil {
			if err == io.EOF {
				break
			}
			return objs, fmt.Errorf("failed to unmarshal manifest: %v", err)
		}
		if len(u.Object) == 0 {
			continue // skip empty object
		}
		objs = append(objs, *u)
	}
	return objs, nil
}
