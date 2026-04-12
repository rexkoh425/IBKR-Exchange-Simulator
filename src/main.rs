use equity_matching_engine::{benchmark, MatchingEngine, OrderRequest, Side};

fn main() {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() == Some("benchmark") {
        let mut events = 1_000_000_usize;
        let mut seed = 2026_u64;
        let mut trials = 5_usize;
        let mut warmup_events = 100_000_usize;
        while let Some(flag) = args.next() {
            let value = args.next().unwrap_or_else(|| usage());
            match flag.as_str() {
                "--events" => events = value.parse().unwrap_or_else(|_| usage()),
                "--seed" => seed = value.parse().unwrap_or_else(|_| usage()),
                "--trials" => trials = value.parse().unwrap_or_else(|_| usage()),
                "--warmup-events" => warmup_events = value.parse().unwrap_or_else(|_| usage()),
                _ => usage(),
            }
        }
        if events == 0 || trials == 0 {
            usage();
        }
        let warmup = benchmark::run_trial(warmup_events.max(1), seed ^ u64::MAX);
        eprintln!(
            "warmup_events={},warmup_seed={},warmup_fingerprint={}",
            warmup.events, warmup.seed, warmup.final_state_fingerprint
        );
        eprintln!(
            "machine={}",
            std::env::var("PROCESSOR_IDENTIFIER").unwrap_or_else(|_| "unknown".into())
        );
        eprintln!("os={}", std::env::consts::OS);
        if let Ok(version) = std::process::Command::new("rustc")
            .arg("--version")
            .output()
        {
            eprintln!("rustc={}", String::from_utf8_lossy(&version.stdout).trim());
        }
        println!("trial,seed,events,submits,cancels,trades,matched_lots,elapsed_seconds,completed_events_per_second,p50_service_ns,p95_service_ns,p99_service_ns,max_service_ns,final_state_fingerprint");
        for trial in 1..=trials {
            let result = benchmark::run_trial(events, seed);
            println!(
                "{trial},{},{},{},{},{},{},{:.6},{:.2},{},{},{},{},{}",
                result.seed,
                result.events,
                result.submit_count,
                result.cancel_count,
                result.trade_count,
                result.matched_lots,
                result.elapsed.as_secs_f64(),
                result.completed_events_per_second,
                result.p50_service_ns,
                result.p95_service_ns,
                result.p99_service_ns,
                result.max_service_ns,
                result.final_state_fingerprint
            );
        }
        return;
    }
    let mut engine = MatchingEngine::new();
    engine.submit(OrderRequest {
        order_id: 1,
        side: Side::Sell,
        price_ticks: 10_001,
        quantity_lots: 5,
    });
    let result = engine.submit(OrderRequest {
        order_id: 2,
        side: Side::Buy,
        price_ticks: 10_005,
        quantity_lots: 3,
    });
    for trade in result.trades {
        println!(
            "trade seq={} maker={} taker={} price_ticks={} quantity_lots={}",
            trade.sequence,
            trade.maker_order_id,
            trade.taker_order_id,
            trade.price_ticks,
            trade.quantity_lots
        );
    }
}

fn usage() -> ! {
    eprintln!("usage: equity-matching-engine [benchmark --events N --seed N --trials N --warmup-events N]");
    std::process::exit(2);
}
