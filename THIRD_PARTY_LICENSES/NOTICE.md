# Third-party notices

This project is licensed under the MIT License; see [`LICENSE`](../LICENSE) at
the repository root. That licence is unchanged. This directory carries the
additional notices required by third-party code that this project adapts.

## casambi-bt

- **Upstream:** <https://github.com/lkempf/casambi-bt>
- **Licence:** Apache License, Version 2.0 — full text in
  [`Apache-2.0-casambi-bt.txt`](Apache-2.0-casambi-bt.txt), reproduced verbatim
  from the upstream `LICENSE` file
- **Adapted in:** [`switch_decoder.py`](../switch_decoder.py)
- **Adapted from:** `CasambiBt/_invocation.py` and `CasambiBt/_switch.py` as
  published in the casambi-bt **0.4.0b4** pre-release

casambi-bt is also the bridge's runtime dependency, pinned at **0.3.2** in
`requirements-server.txt` and installed unmodified from PyPI. Only the switch
event decoder is adapted; that pin is deliberate and is not changed by the
adaptation.

### What was adapted

The invocation frame layout (`flags:u16` big-endian, `opcode:u8`, `origin:u16`,
`target:u16`, `age:u16`, optional `origin_handle:u8`, then `flags & 0x3f`
payload bytes), the opcode ranges that identify a logical control
(`FunctionButtonEvent0..7` at 29–36 and `FunctionNotifyInput0..7` at 64–71), the
split between the button stream (`target` type `0x06`) and the input stream
(`0x12`), and the approach of suppressing retransmissions by frame origin.

Upstream documents that layout as derived from the Casambi Android application.

### Statement of modifications

Required by Apache-2.0 §4(b). Relative to the upstream 0.4.0b4 source, the
adapted code in `switch_decoder.py`:

1. splits decoding into a pure frame parser plus a small stateful decoder class,
   rather than one combined decoder;
2. takes the clock as an injected callable so the retransmit window is testable
   without waiting on real time;
3. emits an object carrying only the three fields this bridge publishes, plus a
   private in-process deduplication identity that is never serialised, instead
   of upstream's fuller event record;
4. drops the `UNKNOWN` phase entirely, so an unrecognised, truncated or
   malformed frame produces no event at all rather than a placeholder;
5. is installed over casambi-bt 0.3.2's `parseSwitchEvents` seam rather than
   replacing the library, so the 0.3.2 discovery, connection lifecycle, unit
   state, light and scene paths continue to run unmodified;
6. adds no logging of frame contents.

No upstream copyright, patent, trademark or attribution notice was removed.
