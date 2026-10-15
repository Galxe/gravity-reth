# Testnet bridge installation runtimes

Both runtimes come from Contracts
`174123adf25395d0490210a3fad46e6018e10366`, compiled on Gravity Dev with
solc `0.8.30+commit.73712a01`, optimizer 200, via IR, EVM Prague and
metadata `bytecodeHash=none`. Historical Gamma artifacts remain separate.

`NativeOracle.bin` is 4010 bytes, Keccak
`13c995f14f5add575e656480be4c734a76bf1b4500097e8258f214c65e174a74`,
SHA256 `b4140d0eb3140d06dec304938594e8c5568b88412a82c51e132696760dce0444`.

`GBridgeReceiver.bin` is the 1212-byte zero-immutable `deployedBytecode.object`
from `out/GBridgeReceiver.sol/GBridgeReceiver.json`, whose SHA256 is
`19fe2be822bfd5d2a8b0fdbe59fc9fd7e5f74a6f67756bb43778fe4a9b5c1ed0`.
Template SHA256:
`f4237b7fc1c7f45248dd1f4e1f7d294c189a52867ce0c9766c2a83927cc77ab7`.

The artifact's `immutableReferences` map AST id `59566` to `trustedBridge` at
byte offsets 84 and 832, and id `59569` to `trustedSourceId` at offsets 153 and
790. Each reference occupies 32 bytes. Getter selectors `0x9b8bd103` and
`0x66430056` independently identify the first bridge/source references.
The installer patches both occurrences of each immutable using the existing
`trustedBridge` and `sourceId` inputs and checks the complete resulting runtime
against `receiverCodeHash`. Getter calls then verify the installed bindings.

The template was extracted from the retained frozen Gravity Dev compiler
artifact. An independent Dev recompilation of the pinned sources with the same
settings reproduced its bytes; the fresh compiler AST and immutable references
confirmed the two named offset groups above. Provenance is archived under the
primary Issue 131 integration evidence, `receiver-install-20261015/artifacts/`.
