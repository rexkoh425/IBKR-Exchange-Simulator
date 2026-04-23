use equity_matching_engine::{benchmark, ibkr, pipeline, MatchingEngine, OrderRequest, Side};

fn main() {
    if std::env::args().nth(1).as_deref() == Some("ibkr-capture") {
        match ibkr::Config::from_env().and_then(ibkr::capture) {
            Ok(()) => return,
            Err(error) => {
                eprintln!("capture failed: {error}");
                std::process::exit(1);
            }
        }
    }
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
    if std::env::args().nth(1).as_deref() == Some("pipeline-benchmark") {
        let mut events = 10_000_000_usize;
        let mut rate = 1_000_000_u64;
        let mut capacity = 65_536_usize;
        let mut trials = 5_usize;
        let mut warmup_events = 100_000_usize;
        let mut queue = "sync".to_owned();
        let mut args = std::env::args().skip(2);
        while let Some(flag) = args.next() {
            let value = args.next().unwrap_or_else(|| usage());
            match flag.as_str() {
                "--events" => events = value.parse().unwrap_or_else(|_| usage()),
                "--rate" => rate = value.parse().unwrap_or_else(|_| usage()),
                "--capacity" => capacity = value.parse().unwrap_or_else(|_| usage()),
                "--trials" => trials = value.parse().unwrap_or_else(|_| usage()),
                "--warmup-events" => warmup_events = value.parse().unwrap_or_else(|_| usage()),
                "--queue" => queue = value,
                _ => usage(),
            }
        }
        if events == 0 || rate == 0 || capacity == 0 || trials == 0 {
            usage();
        }
        if queue != "sync" && queue != "spsc" {
            usage();
        }
        let run = |count, seed| {
            if queue == "spsc" {
                pipeline::run_spsc_trial(count, rate, capacity, seed)
            } else {
                pipeline::run_trial(count, rate, capacity, seed)
            }
        };
        let warmup = run(warmup_events.max(1), 2026 ^ u64::MAX);
        eprintln!(
            "warmup_events={},warmup_completed={},warmup_p99_ns={}",
            warmup.events_offered, warmup.events_completed, warmup.p99_end_to_end_ns
        );
        println!("trial,seed,events_offered,events_completed,capacity,offered_events_per_second,completed_events_per_second,zero_loss,high_water_mark,p50_scheduled_to_complete_ns,p95_scheduled_to_complete_ns,p99_scheduled_to_complete_ns,max_scheduled_to_complete_ns,p99_queue_residence_ns,max_queue_residence_ns,checksum");
        for trial in 1..=trials {
            let seed = 2026 + trial as u64;
            let result = run(events, seed);
            println!(
                "{trial},{seed},{},{},{},{},{:.2},{},{},{},{},{},{},{},{},{}",
                result.events_offered,
                result.events_completed,
                result.capacity,
                result.offered_events_per_second,
                result.completed_events_per_second,
                result.zero_loss,
                result.high_water_mark,
                result.p50_end_to_end_ns,
                result.p95_end_to_end_ns,
                result.p99_end_to_end_ns,
                result.max_end_to_end_ns,
                result.p99_queue_residence_ns,
                result.max_queue_residence_ns,
                result.checksum
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
    eprintln!("usage: equity-matching-engine benchmark|pipeline-benchmark [options]");
    std::process::exit(2);
}
