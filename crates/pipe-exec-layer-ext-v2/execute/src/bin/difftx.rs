//! 差分测试预言机命令行入口。
//!
//! 运行整个 revm 变体矩阵，打印人读表格 + 完整 JSON；对每个停链缺口（gap）在
//! `difftx-repro/` 目录写一个 gnode `send` 兼容的复现文件。有 gap 退出码 3，否则 0。
//!
//! 必须带 `--features difftx` 构建/运行：
//! ```text
//! cargo run -p reth-pipe-exec-layer-ext-v2 --features difftx --bin difftx
//! ```

use reth_pipe_exec_layer_ext_v2::difftx::{self, DiffOutcome};
use std::{fs, path::Path, process::ExitCode};

fn main() -> ExitCode {
    let matrix: Vec<DiffOutcome> = difftx::variant_matrix();

    // —— 人读表格 —— //
    println!("差分预言机矩阵：tx_filter ⊇ revm 交易级校验");
    println!("{:<34} | {:^12} | {:^11} | verdict", "variant", "filter_admit", "revm_reject");
    println!("{}", "-".repeat(90));
    let mut gap_count = 0usize;
    for o in &matrix {
        let verdict = if o.is_gap {
            "CHAIN-HALT GAP"
        } else if o.revm_reject {
            "OK (both reject)"
        } else if o.revm_variant.as_deref().map(|s| s.contains(':')).unwrap_or(false) {
            // 说明行：revm_variant 里带分类前缀（xxx: 原因）。
            "note"
        } else {
            "OK (both admit)"
        };
        if o.is_gap {
            gap_count += 1;
        }
        println!("{:<34} | {:^12} | {:^11} | {}", o.name, o.filter_admit, o.revm_reject, verdict);
    }
    println!("{}", "-".repeat(90));
    println!("总计 {} 行，gap {} 个", matrix.len(), gap_count);

    // —— 完整 JSON —— //
    let json_matrix: Vec<serde_json::Value> = matrix
        .iter()
        .map(|o| {
            serde_json::json!({
                "name": o.name,
                "filter_admit": o.filter_admit,
                "revm_reject": o.revm_reject,
                "revm_variant": o.revm_variant,
                "is_gap": o.is_gap,
            })
        })
        .collect();
    println!("\n完整 JSON:\n{}", serde_json::to_string_pretty(&json_matrix).unwrap());

    // ==================== Tier-2：多笔交易序列执行差分 ==================== //
    println!("\n============ Tier-2：多笔交易序列执行差分（执行诱导 TOCTOU）============");
    let mut seq_gap_count = 0usize;
    let repro_dir = Path::new("difftx-repro");
    for case in difftx::seq_cases() {
        let out = difftx::run_seq_case(&case);
        println!("\n用例 {}: filter_admitted={:?}", out.name, out.filter_admitted);
        if out.exec_gaps.is_empty() {
            println!("  执行缺口: 无（该序列被 filter 拦截或执行存活）");
        } else {
            for g in &out.exec_gaps {
                println!(
                    "  执行缺口: tx#{} sender=0x{} -> {}",
                    g.tx_index,
                    hex::encode(g.sender),
                    g.revm_variant
                );
                seq_gap_count += 1;
            }
            // 写 gnode 可复现工件。
            fs::create_dir_all(repro_dir).expect("创建 difftx-repro 目录");
            let repro = difftx::emit_seq_repro(&case, &out);
            let path = repro_dir.join(format!("seq_{}.json", out.name));
            fs::write(&path, serde_json::to_string_pretty(&repro).unwrap())
                .expect("写序列复现文件");
            eprintln!("写出序列复现文件: {}", path.display());
        }
    }

    // —— 对每个 Tier-1 gap 写复现文件 —— //
    if gap_count > 0 {
        fs::create_dir_all(repro_dir).expect("创建 difftx-repro 目录");
        // 重新按可造用例遍历，找出 gap 并导出（run_case + emit_repro）。
        for case in difftx::craftable_cases_pub() {
            let out = difftx::run_case(&case);
            if out.is_gap {
                let repro = difftx::emit_repro(&case, &out);
                let path = repro_dir.join(format!("{}.json", out.name));
                fs::write(&path, serde_json::to_string_pretty(&repro).unwrap())
                    .expect("写复现文件");
                eprintln!("写出复现文件: {}", path.display());
            }
        }
    }

    println!("\n总判定：Tier-1 gap={} 个，Tier-2 exec_gap={} 个", gap_count, seq_gap_count);
    // 任一 tier 出现 gap 即退出码 3（Tier-2 的 exec_gap 就是已知真链停机的复现）。
    if gap_count > 0 || seq_gap_count > 0 {
        ExitCode::from(3)
    } else {
        ExitCode::from(0)
    }
}
