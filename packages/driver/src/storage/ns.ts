/**
 * Namespace helpers shared by memory / sqlite store backends (Task 9).
 * A namespace is a tuple of path segments; its canonical key is `"a/b/c"`.
 */

/** Canonical string key for a namespace tuple. */
export function nsKey(namespace: readonly string[]): string {
  return namespace.join("/");
}

/** True iff `prefix` is a segment-wise prefix of `ns` (["a"] ⊂ ["a","b"]). */
export function isPrefix(prefix: readonly string[], ns: readonly string[]): boolean {
  if (prefix.length > ns.length) return false;
  for (let i = 0; i < prefix.length; i++) {
    if (prefix[i] !== ns[i]) return false;
  }
  return true;
}
