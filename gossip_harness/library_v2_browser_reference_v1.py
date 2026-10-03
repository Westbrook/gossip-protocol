"""Lossless signed64 browser transport for the prospective cumulative v2 app.

The maintained v1 page is preserved except at its JSON boundary. Exact larger
integers remain BigInt until they are emitted as unquoted JSON decimal tokens.
This authored reference is not independent or candidate-produced evidence.
"""
from __future__ import annotations

from textwrap import dedent

# Inline in the existing page: no dependency, external script, new HTTP route,
# eval, or weakening of the inherited Content-Security-Policy is needed.
LOSSLESS_JSON_SCRIPT = dedent(r'''
    const LosslessJSON = (() => {
      const maximum = 9223372036854775807n;
      const safe = BigInt(Number.MAX_SAFE_INTEGER);
      function counter(value, minimum = 0) {
        if (typeof value === 'number') return Number.isSafeInteger(value) && value >= minimum;
        return typeof value === 'bigint' && value >= BigInt(minimum) && value <= maximum;
      }
      function parse(text) {
        if (typeof text !== 'string') throw new Error('invalid_response');
        let at = 0;
        const invalid = () => { throw new Error('invalid_response'); };
        const space = () => { while (at < text.length && /[\x20\x09\x0a\x0d]/.test(text[at])) at++; };
        function string() {
          const start = at++;
          while (at < text.length) {
            const char = text[at++];
            if (char === '"') {
              try { return JSON.parse(text.slice(start, at)); } catch (_) { return invalid(); }
            }
            if (char === '\\') at++;
            else if (char.charCodeAt(0) < 32) return invalid();
          }
          return invalid();
        }
        function value(depth) {
          if (depth > 128) return invalid();
          space();
          const first = text[at];
          if (first === '"') return string();
          if (first === '{' || first === '[') {
            const object = first === '{';
            const result = object ? Object.create(null) : [];
            const end = object ? '}' : ']';
            at++; space();
            if (text[at] === end) { at++; return result; }
            for (;;) {
              space();
              let key;
              if (object) {
                if (text[at] !== '"') return invalid();
                key = string(); space();
                if (text[at++] !== ':' || Object.hasOwn(result, key)) return invalid();
              }
              const item = value(depth + 1);
              if (object) result[key] = item; else result.push(item);
              space();
              if (text[at] === end) { at++; return result; }
              if (text[at++] !== ',') return invalid();
            }
          }
          for (const [literal, result] of [['true', true], ['false', false], ['null', null]]) {
            if (text.startsWith(literal, at)) { at += literal.length; return result; }
          }
          const match = /^-?(?:0|[1-9][0-9]*)/.exec(text.slice(at));
          if (!match) return invalid();
          at += match[0].length;
          // Product responses contain integer numeric values only. Reject a
          // float/exponent spelling instead of manufacturing an integer token.
          if (/[.eE]/.test(text[at] || ' ')) return invalid();
          const integer = BigInt(match[0]);
          return integer >= -safe && integer <= safe ? Number(integer) : integer;
        }
        const result = value(0);
        space();
        if (at !== text.length) return invalid();
        return result;
      }
      function stringify(value, replacer = null, indentation = 0) {
        if (replacer !== null) throw new Error('invalid_request');
        const gap = typeof indentation === 'number' ? ' '.repeat(Math.max(0, Math.min(10, indentation))) : '';
        const seen = new Set();
        function encode(item, depth) {
          if (depth > 128) throw new Error('invalid_request');
          if (item === null || typeof item === 'boolean' || typeof item === 'string') return JSON.stringify(item);
          if (typeof item === 'bigint') return item.toString(10);
          if (typeof item === 'number') {
            if (!Number.isSafeInteger(item)) throw new Error('invalid_request');
            return JSON.stringify(item);
          }
          if (typeof item !== 'object' || seen.has(item)) throw new Error('invalid_request');
          seen.add(item);
          try {
            const array = Array.isArray(item);
            const entries = array ? item.map(child => encode(child, depth + 1)) :
              Object.keys(item).map(key => JSON.stringify(key) + (gap ? ': ' : ':') + encode(item[key], depth + 1));
            const open = array ? '[' : '{', close = array ? ']' : '}';
            if (!entries.length) return open + close;
            return gap ? open + '\n' + gap.repeat(depth + 1) + entries.join(',\n' + gap.repeat(depth + 1)) +
              '\n' + gap.repeat(depth) + close : open + entries.join(',') + close;
          } finally { seen.delete(item); }
        }
        return encode(value, 0);
      }
      const positive = new Set(['epoch', 'edit_version', 'expected_version']);
      const nonnegative = new Set(['generation', 'expected_generation', 'worker_generation',
        'published_generation', 'target_generation', 'epoch_high_water', 'edit_high_water', 'owner_generation']);
      function validate(value, context = null, depth = 0) {
        if (depth > 128) throw new Error('invalid_response');
        if (value === null || typeof value !== 'object') return value;
        // A running first reindex has no published index generation yet.
        // This exception belongs only to its closed top-level response; a
        // catalog generation or a nested optimistic token remains mandatory.
        const unpublishedIndex = depth === 0 && context === 'reindex' && value.state === 'running' &&
          Object.keys(value).sort().join(',') === 'cursor,generation,processed,state,target_generation,total' &&
          counter(value.target_generation) && Number.isSafeInteger(value.processed) && value.processed >= 0 &&
          Number.isSafeInteger(value.total) && value.total > value.processed &&
          (value.cursor === null || typeof value.cursor === 'string');
        for (const key of Object.keys(value)) {
          const item = value[key];
          if (positive.has(key) && !counter(item, 1)) throw new Error('invalid_response');
          const nullable = (key === 'published_generation' || key === 'target_generation' ||
            (key === 'generation' && unpublishedIndex)) && item === null;
          if (nonnegative.has(key) && !nullable && !counter(item)) throw new Error('invalid_response');
          validate(item, null, depth + 1);
        }
        return value;
      }
      return Object.freeze({parse, stringify, counter, validate});
    })();
''').lstrip("\n").rstrip() + "\n"


def browser_files(base: dict[str, str]) -> dict[str, str]:
    """Return the replacement page without changing the supplied base."""
    browser = base["library/clients/index.html"]
    if browser.count("<script>") != 1 or browser.count("await response.json()") != 1:
        raise ValueError("Frozen cumulative-v1 browser transport anchor changed")
    # Change application uses before inserting the codec, whose native JSON
    # calls handle only strings, booleans and already-safe small integers.
    browser = browser.replace("JSON.stringify(", "LosslessJSON.stringify(")
    browser = browser.replace("JSON.parse(", "LosslessJSON.parse(")
    browser = browser.replace("await response.json()",
                              "LosslessJSON.validate(LosslessJSON.parse(await response.text()), "
                              "url === '/api/maintenance/reindex' ? 'reindex' : null)")
    old = "!Number.isSafeInteger(value.generation) || value.generation < 0"
    if browser.count(old) != 2:
        raise ValueError("Frozen cumulative-v1 recovery/export counter anchors changed")
    browser = browser.replace(old, "!LosslessJSON.counter(value.generation)")
    export = "value = LosslessJSON.parse(new TextDecoder('utf-8', {fatal:true}).decode(bytes));"
    if browser.count(export) != 1:
        raise ValueError("Frozen cumulative-v1 raw export anchor changed")
    browser = browser.replace(export,
                              "value = LosslessJSON.validate(LosslessJSON.parse(new TextDecoder('utf-8', {fatal:true}).decode(bytes)));")
    browser = browser.replace("<script>", "<script>\n" + LOSSLESS_JSON_SCRIPT, 1)
    return {"library/clients/index.html": browser}
