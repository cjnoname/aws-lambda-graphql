export const isAsyncIterableIterator = (
  input: unknown
): input is AsyncIterableIterator<unknown> => {
  return input != null && typeof input[Symbol.asyncIterator] === "function";
};

export const createAsyncIterator = <T>(
  source: any
): {
  next(): Promise<IteratorResult<T>>;
} => {
  if (source == null) {
    return {
      next() {
        return Promise.resolve({ value: undefined, done: true });
      }
    };
  }

  if (typeof source[Symbol.asyncIterator] === "function") {
    return source[Symbol.asyncIterator]();
  }

  if (typeof source.then === "function") {
    let consumed = false;
    return {
      next() {
        if (consumed) {
          return Promise.resolve({ value: undefined, done: true });
        }
        consumed = true;
        return source.then((value: T) => ({ value, done: false }));
      }
    };
  }

  if (typeof source[Symbol.iterator] === "function") {
    const iterator = source[Symbol.iterator]();
    return {
      next() {
        const { value, done } = iterator.next();
        return Promise.resolve({ value, done });
      }
    };
  }

  let consumed = false;
  return {
    next() {
      if (consumed) {
        return Promise.resolve({ value: undefined, done: true });
      }
      consumed = true;
      return Promise.resolve({ value: source, done: false });
    }
  };
};
