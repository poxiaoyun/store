package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/httpclient"
	"xiaoshiai.cn/common/rest/api"
	"xiaoshiai.cn/store"
)

var _ store.PingableStore = &Client{}

// NewRemoteStore creates a REST-backed Store using schema as its local resource
// contract.
func NewRemoteStore(server *url.URL, schema *store.Schema) *Client {
	return &Client{
		cli: &httpclient.Client{
			Server: server,
			OnResponse: func(req *http.Request, resp *http.Response) error {
				if resp.StatusCode < 400 {
					return nil
				}
				body, _ := io.ReadAll(resp.Body)
				statuserr := &errors.Status{}
				if err := json.Unmarshal(body, statuserr); err == nil {
					return statuserr
				}
				return errors.NewBadRequest(string(body))
			},
		},
		schema: schema.Snapshot(),
	}
}

type Client struct {
	cli          *httpclient.Client
	schema       *store.Schema
	scopesPrefix string
}

// Schema implements store.Store.
func (c *Client) Schema() *store.Schema {
	return c.schema.Snapshot()
}

// Capabilities implements store.Store. The REST protocol preserves recursive
// label and field requirements; backend-specific leaf support remains a
// server-side concern reported through Unsupported errors.
func (c *Client) Capabilities() store.Capabilities {
	return store.Capabilities{LabelSelector: true, FieldSelector: true}
}

func (c *Client) Ping(ctx context.Context) error {
	return c.cli.Head("").Send(ctx)
}

// PatchBatch implements store.Store.
func (c *Client) PatchBatch(ctx context.Context, obj store.ObjectList, patch store.PatchBatch, opts ...store.PatchBatchOption) error {
	resource, err := store.GetResource(obj)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	patchdata := patch.Data()
	patchtype := patch.Type()

	options := store.ApplyPatchBatchOptions(opts)
	queries := url.Values{}
	if err := addSelectorQueries(queries, options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	return c.cli.Patch(c.getPath(resource, "")).
		Query("batch", "true").
		Query("status", strconv.FormatBool(false)).
		Queries(queries).
		Body(bytes.NewReader(patchdata), string(patchtype)).
		Return(obj).Send(ctx)
}

// DeleteBatch implements store.Store.
func (c *Client) DeleteBatch(ctx context.Context, obj store.ObjectList, opts ...store.DeleteBatchOption) error {
	resource, err := store.GetResource(obj)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	options := store.ApplyDeleteBatchOptions(opts)
	queries := url.Values{}
	if err := addSelectorQueries(queries, options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	return c.cli.Delete(c.getPath(resource, "")).Queries(queries).Return(obj).Send(ctx)
}

// Count implements store.Store.
func (c Client) Count(ctx context.Context, obj store.Object, opts ...store.CountOption) (int, error) {
	resource, err := store.GetResource(obj)
	if err != nil {
		return 0, errors.NewBadRequest(err.Error())
	}
	options := store.ApplyCountOptions(opts)
	queries := url.Values{}
	if err := addSelectorQueries(queries, options.LabelRequirements, options.FieldRequirements); err != nil {
		return 0, err
	}
	if options.IncludeSubScopes {
		queries.Add("includeSubscopes", "true")
	}
	ret := CountResponse{}
	err = c.cli.Get(c.getPath(resource, "")).Query("count", "true").Queries(queries).Return(&ret).Send(ctx)
	return ret.Count, err
}

// Create implements store.Store.
func (c Client) Create(ctx context.Context, obj store.Object, opts ...store.CreateOption) error {
	resource, err := store.GetResource(obj)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	options := store.ApplyCreateOptions(opts)
	queries := url.Values{}
	if options.TTL != 0 {
		queries.Add("ttl", options.TTL.String())
	}
	return c.cli.Post(c.getPath(resource, "")).Queries(queries).JSON(obj).Return(obj).Send(ctx)
}

// Delete implements store.Store.
func (c Client) Delete(ctx context.Context, obj store.Object, opts ...store.DeleteOption) error {
	resource, err := store.GetResource(obj)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	options := store.ApplyDeleteOptions(opts)
	queries := url.Values{}
	if err := addSelectorQueries(queries, options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	if options.Preconditions != nil {
		if options.Preconditions.UID != nil {
			queries.Add("uid", *options.Preconditions.UID)
		}
		if options.Preconditions.ResourceVersion != nil {
			queries.Add("resourceVersion", strconv.FormatInt(*options.Preconditions.ResourceVersion, 10))
		}
	}
	if options.PropagationPolicy != nil {
		queries.Add("propagationPolicy", string(*options.PropagationPolicy))
	}
	return c.cli.
		Delete(c.getPath(resource, obj.GetID())).
		Queries(queries).
		Return(obj).
		Send(ctx)
}

// Get implements store.Store.
func (c Client) Get(ctx context.Context, name string, obj store.Object, opts ...store.GetOption) error {
	resource, err := store.GetResource(obj)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	options := store.ApplyGetOptions(opts)
	queries := url.Values{}
	if err := addSelectorQueries(queries, options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	if options.ResourceVersion != nil {
		queries.Add("resourceVersion", strconv.FormatInt(*options.ResourceVersion, 10))
	}
	addPreconditionQueries(queries, options.Preconditions)
	if options.Fields != nil {
		queries.Add("fields", strings.Join(options.Fields, ","))
	}
	return c.cli.Get(c.getPath(resource, name)).
		Queries(queries).
		Return(obj).
		Send(ctx)
}

// List implements store.Store.
func (c Client) List(ctx context.Context, list store.ObjectList, opts ...store.ListOption) error {
	resource, err := store.GetResource(list)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	options := store.ApplyListOptions(opts)
	queries := url.Values{}
	if err := addSelectorQueries(queries, options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	if options.IncludeSubScopes {
		queries.Add("includeSubscopes", "true")
	}
	// A cursor read travels as continue/limit; a window read travels as
	// offset/size, which is the protocol's page-shaped window.
	if options.Continue != nil {
		queries.Add("continue", *options.Continue)
	}
	if options.Limit != 0 {
		if options.Continue != nil {
			queries.Add("limit", strconv.Itoa(options.Limit))
		} else {
			queries.Add("size", strconv.Itoa(options.Limit))
		}
	}
	if options.Continue == nil && options.Offset != 0 {
		queries.Add("offset", strconv.Itoa(options.Offset))
	}
	if options.Search != "" {
		queries.Add("search", options.Search)
	}
	if options.Sort != "" {
		queries.Add("sort", options.Sort)
	}
	if options.ResourceVersion != nil {
		queries.Add("resourceVersion", strconv.FormatInt(*options.ResourceVersion, 10))
	}
	if options.Fields != nil {
		queries.Add("fields", strings.Join(options.Fields, ","))
	}
	return c.cli.Get(c.getPath(resource, "")).Queries(queries).Return(list).Send(ctx)
}

// Patch implements store.Store.
func (c Client) Patch(ctx context.Context, obj store.Object, patch store.Patch, opts ...store.PatchOption) error {
	return c.patch(ctx, obj, false, patch, opts...)
}

// Update implements store.Store.
func (c Client) Update(ctx context.Context, obj store.Object, opts ...store.UpdateOption) error {
	return c.update(ctx, obj, false, opts...)
}

// Watch implements store.Store.
func (c Client) Watch(ctx context.Context, obj store.ObjectList, opts ...store.WatchOption) (store.Watcher, error) {
	resource, err := store.GetResource(obj)
	if err != nil {
		return nil, errors.NewBadRequest(err.Error())
	}
	options := store.ApplyWatchOptions(opts)
	queries := url.Values{}
	if err := addSelectorQueries(queries, options.LabelRequirements, options.FieldRequirements); err != nil {
		return nil, err
	}
	if options.ResourceVersion != nil {
		queries.Add("resourceVersion", strconv.FormatInt(*options.ResourceVersion, 10))
	}
	if options.IncludeSubScopes {
		queries.Add("includeSubscopes", "true")
	}
	if options.SendInitialEvents {
		queries.Add("sendInitialEvents", "true")
	}
	resp, err := c.cli.Get(c.getPath(resource, "")).
		Queries(queries).
		Query("watch", "true").Do(ctx)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)

	w := &watcher{
		cancel: cancel,
		result: make(chan store.WatchEvent),
		resp:   resp,
	}
	go w.run(ctx)

	return w, nil
}

type watcher struct {
	cancel context.CancelFunc
	resp   *http.Response
	result chan store.WatchEvent
}

// Events implements store.Watcher.
func (w *watcher) Events() <-chan store.WatchEvent {
	return w.result
}

// Stop implements store.Watcher.
func (w *watcher) Stop() {
	w.cancel()
}

func (w *watcher) run(ctx context.Context) {
	defer w.resp.Body.Close()

	err := api.NewSSEDecode(ctx, w.resp.Body, func(e api.Event) error {
		obj := &store.Unstructured{}
		if err := json.Unmarshal(e.Data, obj); err != nil {
			return err
		}
		w.result <- store.WatchEvent{Type: store.WatchEventType(e.Event), Object: obj}
		return nil
	})
	if err != nil {
		w.result <- store.WatchEvent{Error: err}
		return
	} else {
		w.result <- store.WatchEvent{Error: fmt.Errorf("watcher closed")}
		return
	}
}

// Scope implements store.Store.
func (c Client) Scope(scope ...store.Scope) store.Store {
	prefix := c.scopesPrefix
	for _, s := range scope {
		prefix += "/" + s.Resource + "/" + s.Name
	}
	return &Client{cli: c.cli, schema: c.schema, scopesPrefix: prefix}
}

func (s Client) update(ctx context.Context, obj store.Object, status bool, opts ...store.UpdateOption) error {
	resource, err := store.GetResource(obj)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	options := store.ApplyUpdateOptions(opts)
	queries := url.Values{}
	if options.TTL != 0 {
		queries.Add("ttl", options.TTL.String())
	}
	if err := addSelectorQueries(queries, options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	return s.cli.
		Put(s.getPath(resource, obj.GetID())).
		Query("status", strconv.FormatBool(status)).
		Queries(queries).
		JSON(obj).
		Return(obj).
		Send(ctx)
}

func (c Client) patch(ctx context.Context, obj store.Object, status bool, patch store.Patch, opts ...store.PatchOption) error {
	resource, err := store.GetResource(obj)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	patchdata, err := patch.Data(obj)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	patchtype := patch.Type()

	options := store.ApplyPatchOptions(opts)
	queries := url.Values{}
	if err := addSelectorQueries(queries, options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	addPreconditionQueries(queries, options.Preconditions)
	return c.cli.Patch(c.getPath(resource, obj.GetID())).
		Body(bytes.NewReader(patchdata), string(patchtype)).
		Query("status", strconv.FormatBool(status)).
		Queries(queries).
		Return(obj).
		Send(ctx)
}

func (c Client) getPath(resource, name string) string {
	rpath := c.scopesPrefix + "/" + resource
	if name != "" {
		rpath += "/" + name
	}
	return rpath
}

func addSelectorQueries(queries url.Values, labelRequirements, fieldRequirements store.Requirements) error {
	if err := store.ValidateSelectorRequirements(labelRequirements, fieldRequirements); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	if len(labelRequirements) != 0 {
		queries.Add("labelSelector", labelRequirements.String())
	}
	if len(fieldRequirements) != 0 {
		queries.Add("fieldSelector", fieldRequirements.String())
	}
	return nil
}

// Status implements store.Store.
func (c Client) Status() store.StatusStorage {
	return &statusClient{Client: c}
}

type statusClient struct {
	Client
}

// Patch implements store.StatusStorage.
func (s *statusClient) Patch(ctx context.Context, obj store.Object, patch store.Patch, opts ...store.PatchOption) error {
	return s.Client.patch(ctx, obj, true, patch, opts...)
}

// Update implements store.StatusStorage.
func (s *statusClient) Update(ctx context.Context, obj store.Object, opts ...store.UpdateOption) error {
	return s.Client.update(ctx, obj, true, opts...)
}

func addPreconditionQueries(queries url.Values, conditions *store.Preconditions) {
	if conditions == nil {
		return
	}
	if conditions.UID != nil {
		queries.Set("preconditionUID", *conditions.UID)
	}
	if conditions.ResourceVersion != nil {
		queries.Set("preconditionResourceVersion", strconv.FormatInt(*conditions.ResourceVersion, 10))
	}
}
