# MIGRATION

记录 gravity-reth 与上游 reth 之间的磁盘格式锁定与不兼容点。

## 1. `StoredNibblesSubKey` 磁盘编码锁定(不可滚动升级)

- gravity #149(commit `671680af37`)将 `StoredNibblesSubKey` 的 MDBX 磁盘编码改为
  变长 `[len][packed]` 形式;上游 reth 使用 65 字节右填充(right-padded)编码。
- 两种编码互不兼容:任何跑过 Galxe 网络(在当前 gravity main 编码下写过
  `StoragesTrie` 数据)的节点,**不可滚动升级**到使用上游 65B 编码的二进制,
  否则 trie subkey 读取将得到错误解码结果。
- 本次 v2.3.0 合并维持 gravity 编码(`crates/trie/common/src/nibbles.rs`
  keep-gravity)。若未来切换到上游编码,必须提供离线迁移工具重写受影响的表。

## 2. 上游 `PackedAccountsTrie` / `PackedStoragesTrie` 表(storage-v2 路径)

- 上游 v2.3.0 新增 `PackedAccountsTrie` / `PackedStoragesTrie` 表,属上游
  storage-v2 路径。
- gravity **不消费**这两张表(直至有迁移工具);gravity trie 数据仍走
  `AccountsTrie` / `StoragesTrie` + gravity 编码的 `StoredNibblesSubKey`。
