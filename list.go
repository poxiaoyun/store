package store

import (
	"xiaoshiai.cn/common/meta"

	"xiaoshiai.cn/common/selector"
)

// metaListOption is the parsed public portion of ListOptions. It intentionally
// cannot set Store HTTP protocol fields such as ResourceVersion, Fields, or
// IncludeSubScopes.
type metaListOption struct {
	Offset            int
	Limit             int
	Continue          *string
	Search            string
	Sort              string
	LabelRequirements Requirements
	FieldRequirements Requirements
}

func (option metaListOption) ApplyToList(options *ListOptions) {
	options.Offset = option.Offset
	options.Limit = option.Limit
	options.Continue = option.Continue
	options.Search = option.Search
	options.Sort = option.Sort
	options.LabelRequirements = append(options.LabelRequirements, option.LabelRequirements...)
	options.FieldRequirements = append(options.FieldRequirements, option.FieldRequirements...)
}

// ListOptionsFromMeta converts caller-facing list options into concrete Store
// modifiers, including selector parsing, followed by Store modifiers. A
// positive Limit selects a cursor read; otherwise a positive Size selects a
// window read through the page number, with page values below one treated as
// the first page.
func ListOptionsFromMeta(options meta.ListOptions, modifiers ...ListOption) ([]ListOption, error) {
	resolved := metaListOption{
		Search: options.Search,
		Sort:   options.Sort,
	}
	switch {
	case options.Limit > 0:
		token := options.Continue
		resolved.Continue = &token
		resolved.Limit = options.Limit
	case options.Size > 0:
		resolved.Offset = (max(options.Page, 1) - 1) * options.Size
		resolved.Limit = options.Size
	}
	if options.LabelSelector != "" {
		requirements, err := selector.ParseRequirements(options.LabelSelector)
		if err != nil {
			return nil, err
		}
		resolved.LabelRequirements = requirements
	}
	if options.FieldSelector != "" {
		requirements, err := selector.ParseRequirements(options.FieldSelector)
		if err != nil {
			return nil, err
		}
		resolved.FieldRequirements = requirements
	}
	result := make([]ListOption, 0, 1+len(modifiers))
	result = append(result, resolved)
	return append(result, modifiers...), nil
}
