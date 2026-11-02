package store

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math"
	"reflect"

	"xiaoshiai.cn/common/errors"
)

// ListPage owns the opaque continuation of an offset-based traversal. Backends
// fetch Options.Limit rows (including one lookahead) then call Finish.
// It does not claim a fixed snapshot across requests.
type ListPage struct {
	Options     ListOptions
	limit       int
	fingerprint string
}

type listPosition struct {
	Query  string `json:"query"`
	Offset int    `json:"offset"`
}

// PrepareListPage validates a continuation against the complete query and
// resolves its position. The returned options include a stable identity tie break.
func PrepareListPage(resource string, scopes []Scope, options ListOptions) (*ListPage, error) {
	if options.Continue != "" && options.Offset != 0 {
		return nil, errors.NewBadRequest("continue and offset cannot be combined")
	}
	if options.Limit < 0 || options.Offset < 0 {
		return nil, errors.NewBadRequest("limit and offset must not be negative")
	}
	query := options
	query.Continue, query.Limit, query.Offset = "", 0, 0
	data, err := json.Marshal(struct {
		Resource string
		Scopes   []Scope
		Query    ListOptions
	}{resource, scopes, query})
	if err != nil {
		return nil, errors.NewBadRequest(err.Error())
	}
	digest := sha256.Sum256(data)
	fingerprint := base64.RawURLEncoding.EncodeToString(digest[:])
	if options.Continue != "" {
		data, err := base64.RawURLEncoding.DecodeString(options.Continue)
		if err != nil {
			return nil, errors.NewBadRequest("invalid continue token")
		}
		var position listPosition
		if err := json.Unmarshal(data, &position); err != nil || position.Query != fingerprint || position.Offset <= 0 {
			return nil, errors.NewBadRequest("continue token does not match query")
		}
		options.Offset = position.Offset
	}
	page := &ListPage{Options: options, limit: options.Limit, fingerprint: fingerprint}
	if options.Limit > 0 {
		if options.Limit == math.MaxInt {
			return nil, errors.NewBadRequest("limit is too large")
		}
		page.Options.Limit++
		if options.Offset > math.MaxInt-page.Options.Limit {
			return nil, errors.NewBadRequest("page window overflows")
		}
	}
	// UID uniquely identifies an object even in queries spanning multiple scopes.
	if options.Limit > 0 || options.Continue != "" {
		if page.Options.Sort == "" {
			page.Options.Sort = "id+"
		}
		page.Options.Sort += ",uid+"
	}
	return page, nil
}

// Finish trims the lookahead and supplies a token only when another row exists.
func (p *ListPage) Finish(list ObjectList) error {
	items, err := GetItemsPtr(list)
	if err != nil {
		return err
	}
	value := reflect.ValueOf(items).
		Elem()
	list.SetContinue("")
	if p.limit > 0 && value.Len() > p.limit {
		value.SetLen(p.limit)
		if p.Options.Offset > math.MaxInt-p.limit {
			return errors.NewBadRequest("page position overflows")
		}
		data, err := json.Marshal(listPosition{Query: p.fingerprint, Offset: p.Options.Offset + p.limit})
		if err != nil {
			return err
		}
		list.SetContinue(base64.RawURLEncoding.EncodeToString(data))
	}
	return nil
}
