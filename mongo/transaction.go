package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"xiaoshiai.cn/store"
)

func (s *MongoStorage) Transaction(ctx context.Context,
	fn func(ctx context.Context, store store.Store) error,
	opts ...store.TransactionOption,
) error {
	transactionOptions := store.ApplyTransactionOptions(opts)
	if transactionOptions.Timeout > 0 {
		timoutctx, cancel := context.WithTimeout(ctx, transactionOptions.Timeout)
		defer cancel()
		ctx = timoutctx
	}
	return s.core.db.Client().UseSessionWithOptions(ctx, &options.SessionOptions{}, func(sessionContext mongo.SessionContext) error {
		_, err := sessionContext.WithTransaction(sessionContext, func(sessionContext mongo.SessionContext) (any, error) {
			return nil, fn(sessionContext, s)
		})
		return err
	})
}
