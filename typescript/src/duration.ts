const COMPONENT = /(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|μs|ms|s|m|h)/gy;
const MAX_DURATION_MS = 9_223_372_036_854.775;
const UNIT_MS: Record<string, number> = {
  ns: 1e-6,
  us: 1e-3,
  "µs": 1e-3,
  "μs": 1e-3,
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
};

/** Parse Go's compound duration syntax, returning milliseconds for Node timers. */
export function parseDuration(value: string): number {
  if (value === "0" || value === "+0" || value === "-0") return 0;
  const negative = value.startsWith("-");
  const input = value.startsWith("+") || negative ? value.slice(1) : value;
  if (input.length === 0) throw invalid(value);

  let offset = 0;
  let milliseconds = 0;
  COMPONENT.lastIndex = 0;
  while (offset < input.length) {
    COMPONENT.lastIndex = offset;
    const match = COMPONENT.exec(input);
    if (!match) throw invalid(value);
    const part = Number(match[1]) * UNIT_MS[match[2]!];
    if (!Number.isFinite(part)) throw invalid(value);
    milliseconds += part;
    if (!Number.isFinite(milliseconds) || milliseconds > MAX_DURATION_MS) throw invalid(value);
    offset = COMPONENT.lastIndex;
  }
  if (offset === 0) throw invalid(value);
  return negative ? -milliseconds : milliseconds;
}

function invalid(value: string): Error { return new Error(`time: invalid duration ${JSON.stringify(value)}`); }
