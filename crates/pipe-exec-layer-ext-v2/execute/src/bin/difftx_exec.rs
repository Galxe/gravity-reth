//! serial(revm) ⟷ grevm 执行差分预言机 —— 命令行工具。
//!
//! 跑所有 baseline + adversarial 用例，逐块过**两个后端**（串行 `WrapExecutor` /
//! 并行 `GrevmExecutor`），打印一张对比表；对**任何背离**写一份可复现 artifact 到
//! `difftx-repro/exec_<name>.json`（含区块交易 + 如何在 live 集群复现的指引），
//! 并以退出码 3 收场；无背离则退出 0。
//!
//! ```text
//! cargo run -p reth-pipe-exec-layer-ext-v2 --features difftx_exec --bin difftx_exec
//! ```

use reth_pipe_exec_layer_ext_v2::difftx_exec::{
    exec_adversarial_cases, exec_baseline_cases, run_exec_diff, ExecCase, ExecDiff,
};
use std::io::Write;

fn main() {
    let mut all: Vec<(String, ExecCase, ExecDiff)> = Vec::new();

    for case in exec_baseline_cases() {
        let d = run_exec_diff(&case);
        all.push(("baseline".to_string(), case, d));
    }
    for case in exec_adversarial_cases() {
        let d = run_exec_diff(&case);
        all.push(("adversarial".to_string(), case, d));
    }

    // —— 对比表 —— //
    println!("\n serial(revm) ⟷ grevm 执行差分预言机\n");
    println!(
        "{:<12} {:<40} {:>9} {:>8} {:>9}  divergence",
        "kind", "case", "serial_ok", "grevm_ok", "diverged"
    );
    println!("{}", "-".repeat(110));

    let mut diverged_count = 0usize;
    for (kind, _case, d) in &all {
        println!(
            "{:<12} {:<40} {:>9} {:>8} {:>9}  {}",
            kind,
            d.name,
            d.serial_ok,
            d.grevm_ok,
            d.diverged,
            d.divergence.as_deref().unwrap_or("-")
        );
        if d.diverged {
            diverged_count += 1;
        }
    }

    // —— 对背离写可复现 artifact —— //
    if diverged_count > 0 {
        let dir = std::path::Path::new("difftx-repro");
        if let Err(e) = std::fs::create_dir_all(dir) {
            eprintln!("无法创建 difftx-repro 目录：{e}");
        } else {
            for (kind, case, d) in &all {
                if !d.diverged {
                    continue;
                }
                let path = dir.join(format!("exec_{}.json", d.name));
                match write_repro(&path, kind, case, d) {
                    Ok(()) => println!("  ↳ 已写复现 artifact：{}", path.display()),
                    Err(e) => eprintln!("  ↳ 写 {} 失败：{e}", path.display()),
                }
            }
        }
    }

    println!("\n汇总：{} 个用例，{} 个背离。", all.len(), diverged_count);
    if diverged_count > 0 {
        println!("判决：发现 serial⟷grevm 背离 —— 疑似共识 state-fork。退出码 3。");
        std::process::exit(3);
    } else {
        println!("判决：所有用例 serial==grevm，未发现背离。退出码 0。");
    }
}

/// 写一份 JSON 复现 artifact：用例元信息 + 逐笔交易（RLP hex）+ live 集群复现指引。
fn write_repro(
    path: &std::path::Path,
    kind: &str,
    case: &ExecCase,
    d: &ExecDiff,
) -> std::io::Result<()> {
    use alloy_eips::eip2718::Encodable2718;

    let txs: Vec<serde_json::Value> = case
        .txs
        .iter()
        .zip(case.senders.iter())
        .map(|(tx, sender)| {
            let mut raw = Vec::new();
            tx.encode_2718(&mut raw);
            serde_json::json!({
                "sender": format!("{sender}"),
                "type": tx.tx_type() as u8,
                "raw_2718_hex": format!("0x{}", hex::encode(&raw)),
            })
        })
        .collect();

    let seed: Vec<serde_json::Value> = case
        .seed
        .iter()
        .map(|s| {
            serde_json::json!({
                "addr": format!("{}", s.addr),
                "balance": format!("{}", s.info.balance),
                "nonce": s.info.nonce,
                "has_code": s.info.code.is_some(),
            })
        })
        .collect();

    let doc = serde_json::json!({
        "name": d.name,
        "kind": kind,
        "serial_ok": d.serial_ok,
        "grevm_ok": d.grevm_ok,
        "diverged": d.diverged,
        "divergence": d.divergence,
        "base_fee": case.base_fee,
        "block_gas_limit": case.block_gas_limit,
        "seed": seed,
        "txs": txs,
        "reproduce_on_live_cluster": {
            "note": "两侧后端从同一份 seed 状态 + 同一区块（Prague@ts=0, number=1）执行；\
                     背离即共识 state-fork。用 gnode 把这批交易灌进一个块复现：",
            "steps": [
                "1) 用 seed 预置账户余额/nonce/code（genesis alloc 或直接状态注入）",
                "2) 按 txs[].raw_2718_hex 顺序提交：gnode send --no-wait <rawtx> （逐笔，保持块内顺序）",
                "3) gnode difftx-exec 对该块跑 serial vs grevm 并比对 state root / BundleState / receipts",
                "4) 若 state root 背离 → 确认为链停/分叉级 finding"
            ]
        }
    });

    let mut f = std::fs::File::create(path)?;
    f.write_all(serde_json::to_string_pretty(&doc)?.as_bytes())?;
    f.write_all(b"\n")?;
    Ok(())
}
