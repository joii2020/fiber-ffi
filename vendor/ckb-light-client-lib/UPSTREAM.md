This directory vendors `light-client-lib` from
https://github.com/nervosnetwork/ckb-light-client at commit
`12e29522ab7e078ada704d4ac04cbc0498009b7b`.

It is vendored because Fiber FFI needs a filter-peer selection hook that the
upstream crate does not currently expose. Keep changes limited and marked with
`fiber-ffi` comments so future upstream updates remain reviewable.
