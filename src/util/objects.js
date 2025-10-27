/**
 * Check if a value is a Date instance
 * @param {*} value The value to check.
 * @returns {value is Date} True if value is a Date, false otherwise.
 */
export function isDate(value) {
  return value instanceof Date;
}

/**
 * Check if a value is iterable.
 * @param {*} value The value to check.
 * @returns {value is Iterable} True if value is iterable, false otherwise.
 */
export function isIterable(value) {
  return typeof value[Symbol.iterator] === 'function';
}

/**
 * Return the input value if it passes a test.
 * Otherwise throw an error using the given message generator.
 * @template T
 * @param {T} value The value to check.
 * @param {(value: T) => boolean} test The test function.
 * @param {(value: *) => string} message Message generator.
 * @returns {T} The input value.
 * @throws if the value does not pass the test
 */
export function check(value, test, message) {
  if (test(value)) return value;
  throw new Error(message(value));
}

/**
 * Return the input value if it exists in the provided set.
 * Otherwise throw an error using the given message generator.
 * @template T
 * @param {T} value The value to check.
 * @param {T[] | Record<string,T>} set The set of valid values.
 * @param {(value: *) => string} [message] Message generator.
 * @returns {T} The input value.
 * @throws if the value is not included in the set
 */
export function checkOneOf(value, set, message) {
  set = Array.isArray(set) ? set : Object.values(set);
  return check(
    value,
    (value) => set.includes(value),
    message ?? (() => `${value} must be one of ${set}`)
  );
}

/**
 * Return the first object key that pairs with the given value.
 * @param {Record<string,any>} object The object to search.
 * @param {any} value The value to lookup.
 * @returns {string} The first matching key, or '<Unknown>' if not found.
 */
export function keyFor(object, value) {
  for (const [key, val] of Object.entries(object)) {
    if (val === value) return key;
  }
  return '<Unknown>';
}
