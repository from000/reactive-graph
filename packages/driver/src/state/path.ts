/**
 * Canonical path segments (Task 5 / docs/spec/state-and-transactions.md).
 *
 * A path is an ordered list of segments: string segments address object keys,
 * numeric segments address array indexes. Paths are encoded canonically as
 * `a.b[0].c` so both languages can exchange them and compare them exactly.
 */

export type PathSegment = string | number;
export type Path = readonly PathSegment[];

/** Stringify a path to the canonical form `a.b[0].c`. */
export function pathToString(path: Path): string {
  let out = "";
  for (let i = 0; i < path.length; i++) {
    const seg = path[i]!;
    if (typeof seg === "number") {
      out += `[${seg}]`;
    } else {
      if (i > 0) out += ".";
      out += seg.replace(/\./g, "\\.").replace(/\[/g, "\\[");
    }
  }
  return out;
}

/** Parse the canonical string form back into a Path. */
export function parsePath(canonical: string): Path {
  if (canonical === "") return [];
  const segments: PathSegment[] = [];
  let i = 0;
  let current = "";
  while (i < canonical.length) {
    const ch = canonical[i]!;
    if (ch === "\\") {
      current += canonical[i + 1] ?? "";
      i += 2;
      continue;
    }
    if (ch === "[") {
      let j = i + 1;
      let numStr = "";
      while (j < canonical.length && canonical[j] !== "]") {
        numStr += canonical[j]!;
        j++;
      }
      if (numStr !== "") {
        if (current !== "") {
          segments.push(current);
          current = "";
        }
        segments.push(Number(numStr));
      }
      i = j + 1;
      continue;
    }
    if (ch === ".") {
      if (current !== "") segments.push(current);
      current = "";
      i += 1;
      continue;
    }
    current += ch;
    i += 1;
  }
  if (current !== "") segments.push(current);
  return segments;
}

/** True if `path` is `prefix` or lies beneath it. */
export function isPathUnder(prefix: Path, path: Path): boolean {
  if (prefix.length > path.length) return false;
  for (let i = 0; i < prefix.length; i++) {
    if (prefix[i] !== path[i]) return false;
  }
  return true;
}

export function pathAppend(path: Path, seg: PathSegment): Path {
  return [...path, seg];
}

export function pathParent(path: Path): Path | null {
  if (path.length === 0) return null;
  return path.slice(0, path.length - 1);
}

export function pathEquals(a: Path, b: Path): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

/** Resolve a path against a root object (returns undefined for missing). */
export function getAtPath(root: unknown, path: Path): unknown {
  let cur: unknown = root;
  for (const seg of path) {
    if (cur === null || cur === undefined) return undefined;
    if (typeof seg === "number") {
      if (!Array.isArray(cur)) return undefined;
      cur = (cur as unknown[])[seg];
    } else {
      if (typeof cur !== "object") return undefined;
      cur = (cur as Record<string, unknown>)[seg];
    }
  }
  return cur;
}

/** Set a value at a path, creating containers as needed. Returns true if created. */
export function setAtPath(root: Record<string, unknown>, path: Path, value: unknown): void {
  if (path.length === 0) throw new Error("cannot set at empty path");
  let cur: Record<string, unknown> | unknown[] = root;
  for (let i = 0; i < path.length - 1; i++) {
    const seg = path[i]!;
    const next = path[i + 1]!;
    let child: unknown =
      typeof seg === "number" ? (cur as unknown[])[seg] : (cur as Record<string, unknown>)[seg];
    if (child === null || child === undefined || typeof child !== "object") {
      child = typeof next === "number" ? [] : {};
      if (typeof seg === "number") {
        (cur as unknown[])[seg] = child;
      } else {
        (cur as Record<string, unknown>)[seg] = child;
      }
    }
    cur = child as Record<string, unknown> | unknown[];
  }
  const last = path[path.length - 1]!;
  if (typeof last === "number") {
    (cur as unknown[])[last] = value;
  } else {
    (cur as Record<string, unknown>)[last] = value;
  }
}

/** Delete a value at a path (no-op if missing). */
export function deleteAtPath(root: Record<string, unknown>, path: Path): void {
  if (path.length === 0) return;
  let cur: unknown = root;
  for (let i = 0; i < path.length - 1; i++) {
    const seg = path[i]!;
    if (typeof seg === "number") {
      if (!Array.isArray(cur)) return;
      cur = (cur as unknown[])[seg];
    } else {
      if (typeof cur !== "object" || cur === null) return;
      cur = (cur as Record<string, unknown>)[seg];
    }
    if (cur === undefined) return;
  }
  const last = path[path.length - 1]!;
  if (typeof last === "number") {
    if (!Array.isArray(cur)) return;
    (cur as unknown[]).splice(last, 1);
  } else {
    if (typeof cur !== "object" || cur === null) return;
    delete (cur as Record<string, unknown>)[last];
  }
}
