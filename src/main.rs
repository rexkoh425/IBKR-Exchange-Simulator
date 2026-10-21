use equity_matching_engine::{
    benchmark, gateway, gateway_benchmark, ibkr, journal, market_benchmark, pipeline, replay,
    MatchingEngine, OrderRequest, Side,
};
use std::fs::{File, OpenOptions};
use std::io::{stdin, stdout, BufReader, Write};

fn main() {
    if std::env::args().nth(1).as_deref() == Some("exchange-recover") {
        let mut journal_path = None;
        let mut output_path = None;
        let mut max_order_quantity_lots = u64::MAX;
        let mut max_order_notional_ticks_lots = u128::MAX;
        let mut max_client_open_quantity_lots = u128::MAX;
        let mut max_client_open_notional_ticks_lots = u128::MAX;
        let mut args = std::env::args().skip(2);
        while let Some(flag) = args.next() {
            let value = args.next().unwrap_or_else(|| usage());
            match flag.as_str() {
                "--journal" => journal_path = Some(value),
                "--output" => output_path = Some(value),
                "--max-order-quantity" => {
                    max_order_quantity_lots = value.parse().unwrap_or_else(|_| usage())
                }
                "--max-order-notional" => {
                    max_order_notional_ticks_lots = value.parse().unwrap_or_else(|_| usage())
                }
                "--max-client-open-quantity" => {
                    max_client_open_quantity_lots = value.parse().unwrap_or_else(|_| usage())
                }
                "--max-client-open-notional" => {
                    max_client_open_notional_ticks_lots = value.parse().unwrap_or_else(|_| usage())
                }
                _ => usage(),
            }
        }
        let path = journal_path.unwrap_or_else(|| usage());
        let file = File::open(&path).unwrap_or_else(|error| {
            eprintln!("cannot open exchange journal {path}: {error}");
            std::process::exit(1);
        });
        let recovery = journal::recover(
            BufReader::new(file),
            gateway::GatewayConfig {
                max_order_quantity_lots,
                max_order_notional_ticks_lots,
                max_client_open_quantity_lots,
                max_client_open_notional_ticks_lots,
            },
        )
        .unwrap_or_else(|error| {
            eprintln!("exchange journal recovery failed: {error}");
            std::process::exit(1);
        });
        let mut output: Box<dyn Write> = match output_path {
            Some(path) => Box::new(File::create(&path).unwrap_or_else(|error| {
                eprintln!("cannot create summary {path}: {error}");
                std::process::exit(1);
            })),
            None => Box::new(stdout().lock()),
        };
        serde_json::to_writer(&mut output, &recovery.summary).unwrap_or_else(|error| {
            eprintln!("cannot write recovery summary: {error}");
            std::process::exit(1);
        });
        output.write_all(b"\n").unwrap_or_else(|error| {
            eprintln!("cannot finish recovery summary: {error}");
            std::process::exit(1);
        });
        return;
    }
    if std::env::args().nth(1).as_deref() == Some("exchange-sim") {
        let mut input = None;
        let mut journal_path = None;
        let mut summary_path = None;
        let mut max_order_quantity_lots = u64::MAX;
        let mut max_order_notional_ticks_lots = u128::MAX;
        let mut max_client_open_quantity_lots = u128::MAX;
        let mut max_client_open_notional_ticks_lots = u128::MAX;
        let mut journal_options = journal::JournalOptions::default();
        let mut args = std::env::args().skip(2);
        while let Some(flag) = args.next() {
            let value = args.next().unwrap_or_else(|| usage());
            match flag.as_str() {
                "--input" => input = Some(value),
                "--journal" => journal_path = Some(value),
                "--summary" => summary_path = Some(value),
                "--max-order-quantity" => {
                    max_order_quantity_lots = value.parse().unwrap_or_else(|_| usage())
                }
                "--max-order-notional" => {
                    max_order_notional_ticks_lots = value.parse().unwrap_or_else(|_| usage())
                }
                "--max-client-open-quantity" => {
                    max_client_open_quantity_lots = value.parse().unwrap_or_else(|_| usage())
                }
                "--max-client-open-notional" => {
                    max_client_open_notional_ticks_lots = value.parse().unwrap_or_else(|_| usage())
                }
                "--state-checksum-interval" => {
                    journal_options.full_state_checksum_interval =
                        value.parse().unwrap_or_else(|_| usage())
                }
                "--flush-every" => {
                    journal_options.flush_every_entries = value.parse().unwrap_or_else(|_| usage())
                }
                _ => usage(),
            }
        }
        let reader: Box<dyn std::io::BufRead> = match input {
            Some(path) => Box::new(BufReader::new(File::open(&path).unwrap_or_else(|error| {
                eprintln!("cannot open {path}: {error}");
                std::process::exit(1);
            }))),
            None => Box::new(stdin().lock()),
        };
        let config = gateway::GatewayConfig {
            max_order_quantity_lots,
            max_order_notional_ticks_lots,
            max_client_open_quantity_lots,
            max_client_open_notional_ticks_lots,
        };
        if let Some(path) = journal_path {
            let existing = std::fs::metadata(&path)
                .map(|metadata| metadata.len() > 0)
                .unwrap_or(false);
            let recovery = if existing {
                let file = File::open(&path).unwrap_or_else(|error| {
                    eprintln!("cannot read existing journal {path}: {error}");
                    std::process::exit(1);
                });
                Some(
                    journal::recover(BufReader::new(file), config).unwrap_or_else(|error| {
                        eprintln!("cannot resume exchange journal: {error}");
                        std::process::exit(1);
                    }),
                )
            } else {
                None
            };
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .unwrap_or_else(|error| {
                    eprintln!("cannot append exchange journal {path}: {error}");
                    std::process::exit(1);
                });
            let mut journal = match recovery {
                Some(recovery) => journal::ExchangeJournal::from_recovery_with_options(
                    recovery,
                    file,
                    journal_options,
                ),
                None => journal::ExchangeJournal::with_options(config, file, journal_options),
            };
            let summary = journal::run_jsonl_recorded(reader, stdout().lock(), &mut journal)
                .unwrap_or_else(|error| {
                    eprintln!("exchange simulation failed: {error}");
                    std::process::exit(1);
                });
            if let Some(path) = summary_path {
                let mut file = File::create(&path).unwrap_or_else(|error| {
                    eprintln!("cannot create summary {path}: {error}");
                    std::process::exit(1);
                });
                serde_json::to_writer(&mut file, &summary).unwrap_or_else(|error| {
                    eprintln!("cannot write summary {path}: {error}");
                    std::process::exit(1);
                });
                file.write_all(b"\n").unwrap_or_else(|error| {
                    eprintln!("cannot finish summary {path}: {error}");
                    std::process::exit(1);
                });
            }
        } else {
            let summary =
                gateway::run_jsonl(reader, stdout().lock(), config).unwrap_or_else(|error| {
                    eprintln!("exchange simulation failed: {error}");
                    std::process::exit(1);
                });
            eprintln!(
                "requests={},replayed_requests={},reports={},rejections={},state_fingerprint={:016x}",
                summary.requests,
                summary.replayed_requests,
                summary.reports,
                summary.rejections,
                summary.state_fingerprint
            );
        }
        return;
    }
    if std::env::args().nth(1).as_deref() == Some("exchange-benchmark") {
        let mut events = 100_000_usize;
        let mut trials = 5_usize;
        let mut seed = 20261110_u64;
        let mut core = None;
        let mut journal_options = journal::JournalOptions::default();
        let mut args = std::env::args().skip(2);
        while let Some(flag) = args.next() {
            let value = args.next().unwrap_or_else(|| usage());
            match flag.as_str() {
                "--events" => events = value.parse().unwrap_or_else(|_| usage()),
                "--trials" => trials = value.parse().unwrap_or_else(|_| usage()),
                "--seed" => seed = value.parse().unwrap_or_else(|_| usage()),
                "--core" => core = Some(value.parse::<usize>().unwrap_or_else(|_| usage())),
                "--journal-checksum-interval" => {
                    journal_options.full_state_checksum_interval =
                        value.parse().unwrap_or_else(|_| usage())
                }
                "--journal-flush-every" => {
                    journal_options.flush_every_entries = value.parse().unwrap_or_else(|_| usage())
                }
                _ => usage(),
            }
        }
        if events == 0 || trials == 0 {
            usage();
        }
        let affinity = core
            .map(|id| core_affinity::set_for_current(core_affinity::CoreId { id }))
            .unwrap_or(false);
        if core.is_some() && !affinity {
            eprintln!("failed to pin benchmark thread to requested core {core:?}");
            std::process::exit(2);
        }
        eprintln!("requested_core={core:?},affinity_set={affinity},commands={events},trials={trials},seed={seed},journal_checksum_interval={},journal_flush_every={},workload=frozen_new_orders_seeded_by_splitmix64", journal_options.full_state_checksum_interval, journal_options.flush_every_entries);
        let input = gateway_benchmark::frozen_workload(events, seed);
        let journal_path = std::env::temp_dir().join(format!(
            "exchange-bench-{}-{}.jsonl",
            std::process::id(),
            seed
        ));
        println!("stage,trial,commands,throughput_per_second,p50_ns,p95_ns,p99_ns,p999_ns,max_ns,mismatches,state_checksum,journal_bytes,elapsed_ns");
        let mut expected_gateway_checksum = None;
        for trial in 1..=trials {
            let core_result = benchmark::run_trial(events, seed);
            println!(
                "core,{trial},{},{:.2},{},{},{},{},{},0,{:016x},0,{}",
                core_result.events,
                core_result.completed_events_per_second,
                core_result.p50_service_ns,
                core_result.p95_service_ns,
                core_result.p99_service_ns,
                core_result.p999_service_ns,
                core_result.max_service_ns,
                core_result.final_state_fingerprint,
                core_result.elapsed.as_nanos()
            );
            let mut gateway_result =
                gateway_benchmark::run_gateway(&input, trial).unwrap_or_else(|error| {
                    eprintln!("gateway benchmark failed: {error}");
                    std::process::exit(1);
                });
            let stable_checksum =
                *expected_gateway_checksum.get_or_insert(gateway_result.state_checksum);
            if gateway_result.state_checksum != stable_checksum {
                gateway_result.mismatches += 1;
            }
            println!(
                "{},{},{},{:.2},{},{},{},{},{},{},{:016x},{},{}",
                gateway_result.stage,
                gateway_result.trial,
                gateway_result.commands,
                gateway_result.throughput_per_second,
                gateway_result.p50_ns,
                gateway_result.p95_ns,
                gateway_result.p99_ns,
                gateway_result.p999_ns,
                gateway_result.max_ns,
                gateway_result.mismatches,
                gateway_result.state_checksum,
                gateway_result.journal_bytes,
                gateway_result.elapsed_ns
            );
            let (mut journal_result, journal_bytes) =
                gateway_benchmark::run_journal(&input, trial, &journal_path, journal_options)
                    .unwrap_or_else(|error| {
                        eprintln!("journal benchmark failed: {error}");
                        std::process::exit(1);
                    });
            if journal_result.state_checksum != gateway_result.state_checksum {
                journal_result.mismatches += 1;
            }
            println!(
                "{},{},{},{:.2},{},{},{},{},{},{},{:016x},{},{}",
                journal_result.stage,
                journal_result.trial,
                journal_result.commands,
                journal_result.throughput_per_second,
                journal_result.p50_ns,
                journal_result.p95_ns,
                journal_result.p99_ns,
                journal_result.p999_ns,
                journal_result.max_ns,
                journal_result.mismatches,
                journal_result.state_checksum,
                journal_result.journal_bytes,
                journal_result.elapsed_ns
            );
            let mut replay_result = gateway_benchmark::run_replay(&journal_bytes, trial)
                .unwrap_or_else(|error| {
                    eprintln!("replay benchmark failed: {error}");
                    std::process::exit(1);
                });
            if replay_result.state_checksum != journal_result.state_checksum {
                replay_result.mismatches += 1;
            }
            println!(
                "{},{},{},{:.2},{},{},{},{},{},{},{:016x},{},{}",
                replay_result.stage,
                replay_result.trial,
                replay_result.commands,
                replay_result.throughput_per_second,
                replay_result.p50_ns,
                replay_result.p95_ns,
                replay_result.p99_ns,
                replay_result.p999_ns,
                replay_result.max_ns,
                replay_result.mismatches,
                replay_result.state_checksum,
                replay_result.journal_bytes,
                replay_result.elapsed_ns
            );
        }
        let _ = std::fs::remove_file(journal_path);
        return;
    }
    if std::env::args().nth(1).as_deref() == Some("ibkr-capture") {
        match ibkr::Config::from_env().and_then(ibkr::capture) {
            Ok(()) => return,
            Err(error) => {
                eprintln!("capture failed: {error}");
                std::process::exit(1);
            }
        }
    }
    if std::env::args().nth(1).as_deref() == Some("market-replay") {
        let mut input = None;
        let mut speed = 1000.0_f64;
        let mut step = false;
        let mut expected_events: Option<u64> = None;
        let mut expected_checksum: Option<u64> = None;
        let mut args = std::env::args().skip(2);
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--input" => input = args.next(),
                "--speed" => {
                    speed = args
                        .next()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| usage())
                }
                "--step" => step = true,
                "--expected-events" => {
                    expected_events = Some(
                        args.next()
                            .and_then(|v| v.parse().ok())
                            .unwrap_or_else(|| usage()),
                    )
                }
                "--expected-checksum" => {
                    expected_checksum = Some(
                        args.next()
                            .and_then(|v| u64::from_str_radix(v.trim_start_matches("0x"), 16).ok())
                            .unwrap_or_else(|| usage()),
                    )
                }
                _ => usage(),
            }
        }
        let input = input.unwrap_or_else(|| usage());
        let file = File::open(&input).unwrap_or_else(|error| {
            eprintln!("cannot open {input}: {error}");
            std::process::exit(1);
        });
        let summary = replay::replay_reader(
            BufReader::new(file),
            stdin().lock(),
            &mut stdout().lock(),
            replay::ReplayOptions { speed, step },
        )
        .unwrap_or_else(|error| {
            eprintln!("market replay failed: {error}");
            std::process::exit(1);
        });
        if expected_events.is_some_and(|expected| expected != summary.events_applied) {
            eprintln!(
                "event count mismatch: expected {}, applied {}",
                expected_events.unwrap(),
                summary.events_applied
            );
            std::process::exit(1);
        }
        if expected_checksum.is_some_and(|expected| expected != summary.checksum) {
            eprintln!(
                "event checksum mismatch: expected {:016x}, got {:016x}",
                expected_checksum.unwrap(),
                summary.checksum
            );
            std::process::exit(1);
        }
        println!("\nevents_applied={},checksum={:016x},state_fingerprint={:016x},quotes={},depth_events={},gaps={},out_of_order_receive_timestamps={},incomplete={},stopped_by_user={}", summary.events_applied, summary.checksum, summary.state_fingerprint, summary.quotes, summary.depth_updates, summary.gaps, summary.out_of_order_receive_timestamps, summary.incomplete, summary.stopped_by_user);
        return;
    }
    if std::env::args().nth(1).as_deref() == Some("market-data-benchmark") {
        let mut events = 1_000_000_usize;
        let mut output = None;
        let mut args = std::env::args().skip(2);
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--events" => {
                    events = args
                        .next()
                        .and_then(|value| value.parse().ok())
                        .unwrap_or_else(|| usage())
                }
                "--output" => output = args.next(),
                _ => usage(),
            }
        }
        if events == 0 {
            usage();
        }
        let output = output.map(std::path::PathBuf::from).unwrap_or_else(|| {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock is after Unix epoch")
                .as_nanos();
            std::env::temp_dir().join(format!(
                "market-data-benchmark-{}-{unique}.jsonl",
                std::process::id()
            ))
        });
        let result = market_benchmark::run(events, &output).unwrap_or_else(|error| {
            eprintln!("market data benchmark failed: {error}");
            std::process::exit(1);
        });
        eprintln!("output_file={}", output.display());
        println!("stage,events,elapsed_ns,events_per_second,p50_ns,p95_ns,p99_ns,p999_ns,max_ns");
        for (name, stage) in [
            ("normalize", &result.normalization),
            ("persist_jsonl", &result.persistence),
            ("decode_jsonl", &result.decode),
            ("replay", &result.replay),
            ("in_memory_end_to_end", &result.in_memory_end_to_end),
        ] {
            println!(
                "{name},{},{},{:.2},{},{},{},{},{}",
                stage.events,
                stage.elapsed_ns,
                stage.events_per_second,
                stage.p50_ns,
                stage.p95_ns,
                stage.p99_ns,
                stage.p999_ns,
                stage.max_ns
            );
        }
        println!(
            "summary,source_callbacks={},normalized_events={},file_bytes={},flush_ns={},checksum={:016x},state_fingerprint={:016x},deterministic={},incomplete={}",
            result.source_callbacks,
            result.normalized_events,
            result.file_bytes,
            result.persistence_flush_ns,
            result.event_checksum,
            result.state_fingerprint,
            result.deterministic_replay,
            result.incomplete
        );
        return;
    }
    if std::env::args().nth(1).as_deref() == Some("market-data-spsc-benchmark") {
        let mut events = 1_000_000_usize;
        let mut rate = 1_000_000_u64;
        let mut capacity = 65_536_usize;
        let mut sample_interval = 64_usize;
        let mut burst_size = 1_usize;
        let mut warmup_events = 100_000_usize;
        let mut producer_core = None;
        let mut consumer_core = None;
        let mut retain_raw_payload = true;
        let mut args = std::env::args().skip(2);
        while let Some(flag) = args.next() {
            if flag == "--no-raw-payload" {
                retain_raw_payload = false;
                continue;
            }
            let value = args.next().unwrap_or_else(|| usage());
            match flag.as_str() {
                "--events" => events = value.parse().unwrap_or_else(|_| usage()),
                "--rate" => rate = value.parse().unwrap_or_else(|_| usage()),
                "--capacity" => capacity = value.parse().unwrap_or_else(|_| usage()),
                "--sample-interval" => sample_interval = value.parse().unwrap_or_else(|_| usage()),
                "--burst-size" => burst_size = value.parse().unwrap_or_else(|_| usage()),
                "--warmup-events" => warmup_events = value.parse().unwrap_or_else(|_| usage()),
                "--producer-core" => {
                    producer_core = Some(value.parse().unwrap_or_else(|_| usage()))
                }
                "--consumer-core" => {
                    consumer_core = Some(value.parse().unwrap_or_else(|_| usage()))
                }
                _ => usage(),
            }
        }
        if events == 0 || rate == 0 || capacity == 0 || burst_size == 0 {
            usage();
        }
        let options = |event_count, telemetry| market_benchmark::MarketSpscOptions {
            events: event_count,
            offered_rate: rate,
            capacity,
            sample_interval: telemetry,
            burst_size,
            producer_core,
            consumer_core,
            retain_raw_payload,
        };
        if warmup_events > 0 {
            let warmup =
                market_benchmark::run_spsc(options(warmup_events, 0)).unwrap_or_else(|error| {
                    eprintln!("market SPSC warmup failed: {error}");
                    std::process::exit(1);
                });
            eprintln!(
                "warmup_callbacks={},warmup_completed={},warmup_checksum={:016x}",
                warmup.source_callbacks, warmup.applied_events, warmup.checksum
            );
        }
        let result =
            market_benchmark::run_spsc(options(events, sample_interval)).unwrap_or_else(|error| {
                eprintln!("market SPSC benchmark failed: {error}");
                std::process::exit(1);
            });
        eprintln!(
            "machine={},os={}",
            std::env::var("PROCESSOR_IDENTIFIER").unwrap_or_else(|_| "unknown".into()),
            std::env::consts::OS
        );
        if let Ok(version) = std::process::Command::new("rustc")
            .arg("--version")
            .output()
        {
            eprintln!("rustc={}", String::from_utf8_lossy(&version.stdout).trim());
        }
        println!("metric,value");
        println!("source_callbacks,{}", result.source_callbacks);
        println!("queue_events,{}", result.queue_events);
        println!("applied_events,{}", result.applied_events);
        println!("capacity,{}", result.capacity);
        println!("offered_rate,{}", result.offered_rate);
        println!(
            "completed_events_per_second,{:.2}",
            result.completed_events_per_second
        );
        println!("queue_full_retries,{}", result.queue_full_retries);
        println!("sampled_high_water,{}", result.sampled_high_water);
        println!("producer_pin_succeeded,{}", result.producer_pin_succeeded);
        println!("consumer_pin_succeeded,{}", result.consumer_pin_succeeded);
        println!("event_order_errors,{}", result.event_order_errors);
        println!("checksums_match,{}", result.checksums_match);
        println!("checksum,{:016x}", result.checksum);
        println!("state_fingerprint,{:016x}", result.state_fingerprint);
        println!("incomplete,{}", result.incomplete);
        print_distribution("producer_release_lateness", &result.release_lateness);
        print_distribution("normalization", &result.normalization);
        print_distribution("queue_push", &result.queue_push);
        print_distribution("queue_residence", &result.queue_residence);
        print_distribution("consumer_replay", &result.consumer_replay);
        print_distribution("scheduled_to_completion", &result.scheduled_to_completion);
        return;
    }
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() == Some("benchmark") {
        let mut events = 1_000_000_usize;
        let mut seed = 2026_u64;
        let mut trials = 5_usize;
        let mut warmup_events = 100_000_usize;
        let mut core = None;
        while let Some(flag) = args.next() {
            let value = args.next().unwrap_or_else(|| usage());
            match flag.as_str() {
                "--events" => events = value.parse().unwrap_or_else(|_| usage()),
                "--seed" => seed = value.parse().unwrap_or_else(|_| usage()),
                "--trials" => trials = value.parse().unwrap_or_else(|_| usage()),
                "--warmup-events" => warmup_events = value.parse().unwrap_or_else(|_| usage()),
                "--core" => core = Some(value.parse::<usize>().unwrap_or_else(|_| usage())),
                _ => usage(),
            }
        }
        if events == 0 || trials == 0 {
            usage();
        }
        let affinity = core
            .map(|id| core_affinity::set_for_current(core_affinity::CoreId { id }))
            .unwrap_or(false);
        if core.is_some() && !affinity {
            eprintln!("failed to pin benchmark thread to requested core {core:?}");
            std::process::exit(2);
        }
        let warmup = benchmark::run_trial(warmup_events.max(1), seed ^ u64::MAX);
        eprintln!(
            "warmup_events={},warmup_seed={},warmup_fingerprint={}",
            warmup.events, warmup.seed, warmup.final_state_fingerprint
        );
        eprintln!(
            "machine={},requested_core={core:?},affinity_set={affinity}",
            std::env::var("PROCESSOR_IDENTIFIER").unwrap_or_else(|_| "unknown".into()),
        );
        eprintln!("os={}", std::env::consts::OS);
        if let Ok(version) = std::process::Command::new("rustc")
            .arg("--version")
            .output()
        {
            eprintln!("rustc={}", String::from_utf8_lossy(&version.stdout).trim());
        }
        println!("trial,seed,events,submits,cancels,trades,matched_lots,elapsed_seconds,completed_events_per_second,p50_service_ns,p95_service_ns,p99_service_ns,p999_service_ns,max_service_ns,final_state_fingerprint");
        for trial in 1..=trials {
            let result = benchmark::run_trial(events, seed);
            println!(
                "{trial},{},{},{},{},{},{},{:.6},{:.2},{},{},{},{},{},{}",
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
                result.p999_service_ns,
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
        let mut producer_core = None;
        let mut consumer_core = None;
        let mut occupancy_sample_interval = 64_usize;
        let mut burst_size = 1_usize;
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
                "--producer-core" => {
                    producer_core = Some(value.parse().unwrap_or_else(|_| usage()))
                }
                "--consumer-core" => {
                    consumer_core = Some(value.parse().unwrap_or_else(|_| usage()))
                }
                "--occupancy-sample-interval" => {
                    occupancy_sample_interval = value.parse().unwrap_or_else(|_| usage())
                }
                "--burst-size" => burst_size = value.parse().unwrap_or_else(|_| usage()),
                _ => usage(),
            }
        }
        if events == 0
            || rate == 0
            || capacity == 0
            || trials == 0
            || occupancy_sample_interval == 0
            || burst_size == 0
        {
            usage();
        }
        if queue != "sync" && queue != "spsc" {
            usage();
        }
        if queue == "sync" && burst_size != 1 {
            usage();
        }
        eprintln!(
            "available_cpu_ids={:?}",
            core_affinity::get_core_ids()
                .unwrap_or_default()
                .iter()
                .map(|core| core.id)
                .collect::<Vec<_>>()
        );
        let run = |count, seed| {
            if queue == "spsc" {
                pipeline::run_spsc_trial_with_affinity_sampling_and_burst(
                    count,
                    rate,
                    capacity,
                    seed,
                    producer_core,
                    consumer_core,
                    pipeline::SpscBenchmarkOptions {
                        occupancy_sample_interval,
                        burst_size,
                    },
                )
            } else {
                pipeline::run_trial_with_affinity(
                    count,
                    rate,
                    capacity,
                    seed,
                    producer_core,
                    consumer_core,
                )
            }
        };
        let warmup = run(warmup_events.max(1), 2026 ^ u64::MAX);
        eprintln!(
            "warmup_events={},warmup_completed={},warmup_p99_ns={}",
            warmup.events_offered, warmup.events_completed, warmup.p99_end_to_end_ns
        );
        println!("trial,seed,events_offered,events_completed,capacity,offered_events_per_second,completed_events_per_second,zero_loss,producer_pin_succeeded,consumer_pin_succeeded,occupancy_sample_interval,high_water_mark,p50_scheduled_to_complete_ns,p95_scheduled_to_complete_ns,p99_scheduled_to_complete_ns,p999_scheduled_to_complete_ns,max_scheduled_to_complete_ns,p99_queue_residence_ns,p999_queue_residence_ns,max_queue_residence_ns,checksum,ordering_errors,queue_full_retries,burst_size");
        for trial in 1..=trials {
            let seed = 2026 + trial as u64;
            let result = run(events, seed);
            println!(
                "{trial},{seed},{},{},{},{},{:.2},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                result.events_offered,
                result.events_completed,
                result.capacity,
                result.offered_events_per_second,
                result.completed_events_per_second,
                result.zero_loss,
                result.producer_pin_succeeded,
                result.consumer_pin_succeeded,
                if queue == "spsc" {
                    occupancy_sample_interval
                } else {
                    0
                },
                result.high_water_mark,
                result.p50_end_to_end_ns,
                result.p95_end_to_end_ns,
                result.p99_end_to_end_ns,
                result.p999_end_to_end_ns,
                result.max_end_to_end_ns,
                result.p99_queue_residence_ns,
                result.p999_queue_residence_ns,
                result.max_queue_residence_ns,
                result.checksum,
                result.ordering_errors,
                result
                    .queue_full_retries
                    .map_or_else(String::new, |value| value.to_string()),
                burst_size
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
    eprintln!("usage: equity-matching-engine benchmark|exchange-benchmark|pipeline-benchmark|market-data-benchmark|market-data-spsc-benchmark|ibkr-capture|market-replay|exchange-sim|exchange-recover [options]");
    std::process::exit(2);
}

fn print_distribution(
    name: &str,
    distribution: &equity_matching_engine::market_benchmark::LatencyDistribution,
) {
    println!("{name}_samples,{}", distribution.samples);
    println!("{name}_p50_ns,{}", distribution.p50_ns);
    println!("{name}_p95_ns,{}", distribution.p95_ns);
    println!("{name}_p99_ns,{}", distribution.p99_ns);
    println!("{name}_p999_ns,{}", distribution.p999_ns);
    println!("{name}_max_ns,{}", distribution.max_ns);
}
