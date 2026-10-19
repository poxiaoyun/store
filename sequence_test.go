package store_test

import (
	"context"
	"fmt"

	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/inmemory"
)

func ExampleSequence() {
	var sequence store.Sequence = inmemory.NewSequence()
	for _, namespace := range []string{"issues/one", "issues/one", "issues/two"} {
		value, err := sequence.Next(context.Background(), namespace)
		if err != nil {
			panic(err)
		}
		fmt.Println(value)
	}
	// Output:
	// 1
	// 2
	// 1
}
