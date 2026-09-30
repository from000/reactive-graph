/**
 * RGP/1 framing: four-byte big-endian byte length + one MessagePack envelope
 * (docs/spec/rgp-1.md §1). Cross-language: Python mirror in protocol.py.
 */

const HEADER_BYTES = 4;

export class ProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProtocolError";
  }
}

/**
 * Encode a frame: [4-byte big-endian length][payload].
 * The payload is a MessagePack-encoded envelope (Uint8Array).
 */
export function encodeFrame(payload: Uint8Array): Uint8Array {
  const out = new Uint8Array(HEADER_BYTES + payload.length);
  const view = new DataView(out.buffer);
  view.setUint32(0, payload.length, false); // big-endian
  out.set(payload, HEADER_BYTES);
  return out;
}

/**
 * Incremental frame decoder. Feed arbitrary chunks via `push`; returns complete
 * frames in wire order, buffering partial and multi-frame chunks.
 */
export class FrameDecoder {
  private buffer: Uint8Array;
  private readonly maxFrameBytes: number;

  constructor(opts?: { maxFrameBytes?: number }) {
    this.maxFrameBytes = opts?.maxFrameBytes ?? 32 * 1024 * 1024;
    this.buffer = new Uint8Array(0);
  }

  push(chunk: Uint8Array): Uint8Array[] {
    const frames: Uint8Array[] = [];
    // append chunk to buffer
    const merged = new Uint8Array(this.buffer.length + chunk.length);
    merged.set(this.buffer, 0);
    merged.set(chunk, this.buffer.length);
    this.buffer = merged;

    while (true) {
      if (this.buffer.length < HEADER_BYTES) break;
      const length = new DataView(
        this.buffer.buffer,
        this.buffer.byteOffset,
        HEADER_BYTES,
      ).getUint32(0, false);
      if (length > this.maxFrameBytes) {
        this.reset();
        throw new ProtocolError(
          `frame length ${length} exceeds maxFrameBytes ${this.maxFrameBytes}`,
        );
      }
      if (this.buffer.length < HEADER_BYTES + length) break;
      const frame = this.buffer.slice(HEADER_BYTES, HEADER_BYTES + length);
      this.buffer = this.buffer.slice(HEADER_BYTES + length);
      frames.push(frame);
    }
    return frames;
  }

  reset(): void {
    this.buffer = new Uint8Array(0);
  }
}
