import type { GraphQLResolveInfo } from "graphql";
import { $$asyncIterator } from "iterall";
import type { IContext, SubscribeResolveFn } from "./types";
import type { SubscriptionSubscribeFunc } from "node_modules/type-graphql/build/typings/decorators/types";

export type FilterFn = (
  rootValue?: any,
  args?: any,
  context?: IContext,
  info?: GraphQLResolveInfo
) => boolean | Promise<boolean>;

function withFilter(
  asyncIteratorFn: SubscribeResolveFn,
  filterFn: FilterFn
): SubscriptionSubscribeFunc {
  return async ({ source: rootValue, args, context, info }) => {
    const asyncIterator = await asyncIteratorFn(rootValue, args, context, info);

    const getNextPromise = (): Promise<any> => {
      return asyncIterator.next().then(payload => {
        if (payload.done === true) {
          return payload;
        }

        return Promise.resolve(filterFn(payload.value, args, context, info)).then(filterResult => {
          if (filterResult === true) {
            return payload;
          }

          // Skip the current value and wait for the next one
          return getNextPromise();
        });
      });
    };

    return {
      next() {
        return getNextPromise();
      },
      return() {
        return asyncIterator.return!();
      },
      throw(error: any) {
        return asyncIterator.throw!(error);
      },
      [$$asyncIterator]() {
        return this;
      }
    } as any;
  };
}

export { withFilter };

export default withFilter;
