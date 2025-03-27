// Helper function to check if an object is an async iterable
export const isAsyncIterable = (value: any): value is AsyncIterable<any> => {
  return typeof value === "object" && value !== null && Symbol.asyncIterator in value;
};
