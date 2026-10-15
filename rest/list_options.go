package rest

import (
	"net/http"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/common/rest/api"
	"xiaoshiai.cn/store"
)

// ListOptionsFromRequest converts public request list options, including
// recursive label and field selectors, into Store modifiers. Caller-owned
// defaults apply before selector conversion.
func ListOptionsFromRequest(r *http.Request, defaults ...meta.ListOption) ([]store.ListOption, error) {
	requestOptions, err := api.GetListOptions(r, defaults...)
	if err != nil {
		return nil, err
	}
	options, err := store.ListOptionsFromMeta(requestOptions)
	if err != nil {
		return nil, errors.NewBadRequest(err.Error())
	}
	return options, nil
}
