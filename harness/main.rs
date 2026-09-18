//! `sim` -- the command line front end.
//!
//! ```text
//! sim run     [--seed N]                 one run, reported in full
//! sim sweep   [--seeds N] [--threads N]  many seeds, hunting for failures
//! sim replay  --seed N [--trace LEVEL]   re-run a seed, verbose and verified
//! sim shrink  --seed N                   cut a failure down to a minimal repro
//! sim demo                               a quick tour of all of the above
//! ```
//!
//! Exit status is 0 when nothing failed, 1 when something did, and 2 for a
//! usage error -- so a sweep can be dropped straight into CI.

use harness::replay::{replay, write_trace};
use harness::shrink::{describe, shrink};
use harness::sweep::{default_threads, sweep, SweepConfig};
use harness::{run, SimConfig};
use kvstore::raft::InjectedBug;
use simcore::trace::Level;
use simcore::{MILLIS, SECONDS};
use std::collections::BTreeMap;

const USAGE: &str = "\
sim -- deterministic simulation testing for a replicated key-value store

USAGE:
    sim <command> [options]

COMMANDS:
    run       Run one simulation and report the result
    sweep     Run many seeds in parallel and report failing ones
    replay    Re-run one seed with tracing, and verify determinism
    shrink    Reduce a failing configuration to a minimal reproduction
    demo      A short end-to-end demonstration

COMMON OPTIONS:
    --seed N            Seed to run (default 1)
    --servers N         Cluster size (default 3)
    --clients N         Concurrent clients (default 4)
    --keys N            Distinct keys in the workload (default 6)
    --duration MS       How long faults are injected for (default 20000)
    --settle MS         Quiet period after faults stop (default 15000)
    --benign            No faults at all: a perfect network and honest disks
    --no-liveness       Do not require progress after recovery
    --quorum-loss       Let a majority go down at once; checks safety only
    --bug NAME          Inject a deliberate defect, to prove the checkers see it:
                        ack-before-sync, commit-any-term, vote-before-sync,
                        no-dedup, truncate-on-any-append
    --trace LEVEL       off | error | warn | info | debug (default off)
    --check-durability-every N
                        Events between disk-level durability sweeps (default 200;
                        set to 1 to pinpoint exactly when a claim goes bad)

SWEEP OPTIONS:
    --seeds N           How many seeds to run (default 200)
    --from N            First seed (default 1)
    --threads N         Worker threads (default: number of cores)
    --stop-after N      Stop once N failures are found (default: run them all)
    --verbose           Print a line per run, not just failures

REPLAY OPTIONS:
    --runs N            Extra runs used to verify determinism (default 2)
    --out FILE          Write the trace to a file

SHRINK OPTIONS:
    --budget N          Maximum candidate runs (default 120)

EXIT STATUS:
    0  no failures    1  failures found    2  usage error
";

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.is_empty() {
        print!("{USAGE}");
        std::process::exit(2);
    }
    let command = argv[0].clone();
    let args = match Args::parse(&argv[1..]) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}\n");
            print!("{USAGE}");
            std::process::exit(2);
        }
    };

    let code = match command.as_str() {
        "run" => cmd_run(&args),
        "sweep" => cmd_sweep(&args),
        "replay" => cmd_replay(&args),
        "shrink" => cmd_shrink(&args),
        "demo" => cmd_demo(),
        "-h" | "--help" | "help" => {
            print!("{USAGE}");
            0
        }
        other => {
            eprintln!("error: unknown command {other:?}\n");
            print!("{USAGE}");
            2
        }
    };
    std::process::exit(code);
}

// ---- commands -----------------------------------------------------------

fn cmd_run(args: &Args) -> i32 {
    let cfg = config(args);
    println!("running:\n{}\n", describe(&cfg));
    let outcome = run(cfg);
    println!("{}", outcome.detail());
    if let Some(trace) = &outcome.trace {
        println!("--- trace ---\n{trace}");
    }
    i32::from(outcome.failed())
}

fn cmd_sweep(args: &Args) -> i32 {
    let base = config(args);
    let cfg = SweepConfig {
        start_seed: args.u64("from", 1),
        count: args.u64("seeds", 200),
        threads: args.usize("threads", default_threads()),
        stop_after: args.usize("stop-after", 0),
        verbose: args.has("verbose"),
        base,
    };
    println!(
        "sweeping seeds {}..{} on {} threads\n{}\n",
        cfg.start_seed,
        cfg.start_seed + cfg.count - 1,
        cfg.threads,
        describe(&cfg.base)
    );
    let result = sweep(cfg);
    print!("{}", result.render());
    i32::from(!result.passed())
}

fn cmd_replay(args: &Args) -> i32 {
    let cfg = config(args);
    let level = args.level().unwrap_or(Level::Info);
    let runs = args.usize("runs", 2);
    println!("replaying seed {} at trace level {level:?}\n", cfg.seed);
    let result = replay(cfg, level, runs);
    print!("{}", result.render());

    match args.get("out") {
        Some(path) => match write_trace(path, &result) {
            Ok(()) => println!("\ntrace written to {path}"),
            Err(e) => eprintln!("\ncould not write {path}: {e}"),
        },
        None => {
            if !result.trace.is_empty() {
                println!("\n--- trace ---\n{}", result.trace);
            }
        }
    }

    if !result.deterministic() {
        return 1;
    }
    i32::from(result.outcome.failed())
}

fn cmd_shrink(args: &Args) -> i32 {
    let cfg = config(args);
    let budget = args.usize("budget", 120);
    match shrink(cfg, budget, |line| println!("{line}")) {
        None => {
            println!("nothing to shrink: this configuration passes");
            0
        }
        Some(result) => {
            println!("\n{}", result.render());
            1
        }
    }
}

fn cmd_demo() -> i32 {
    println!("== 1. one run in a perfect world ==\n");
    let clean = run(SimConfig::benign(1));
    println!("{}\n", clean.detail());

    println!("== 2. the same cluster, under crashes, partitions and torn writes ==\n");
    let mut hostile = SimConfig::with_seed(1);
    hostile.duration = 8 * SECONDS;
    hostile.settle = 10 * SECONDS;
    hostile.drain = 3 * SECONDS;
    hostile.normalise();
    let chaos = run(hostile.clone());
    println!("{}\n", chaos.detail());

    println!("== 3. determinism: the same seed, run three times ==\n");
    let r = replay(hostile.clone(), Level::Off, 2);
    for (i, f) in r.fingerprints.iter().enumerate() {
        println!("  run {i}: fingerprint {f}");
    }
    println!(
        "  {}\n",
        if r.deterministic() {
            "all runs identical"
        } else {
            "DIVERGED -- the simulator is not reproducible"
        }
    );

    println!("== 4. a small sweep ==\n");
    let result = sweep(SweepConfig {
        base: hostile,
        start_seed: 1,
        count: 24,
        threads: default_threads(),
        stop_after: 0,
        verbose: false,
    });
    print!("{}", result.render());

    let failed = clean.failed() || chaos.failed() || !result.passed() || !r.deterministic();
    i32::from(failed)
}

// ---- configuration ------------------------------------------------------

fn config(args: &Args) -> SimConfig {
    let seed = args.u64("seed", 1);
    let mut cfg = if args.has("benign") {
        SimConfig::benign(seed)
    } else {
        SimConfig::with_seed(seed)
    };
    cfg.servers = args.usize("servers", cfg.servers);
    cfg.clients = args.usize("clients", cfg.clients);
    cfg.workload.keys = args.usize("keys", cfg.workload.keys);
    cfg.duration = args.u64("duration", cfg.duration / MILLIS) * MILLIS;
    cfg.settle = args.u64("settle", cfg.settle / MILLIS) * MILLIS;
    cfg.drain = args.u64("drain", cfg.drain / MILLIS) * MILLIS;
    if let Some(l) = args.level() {
        cfg.trace_level = l;
    }
    if args.has("no-liveness") {
        cfg.check_liveness = false;
    }
    if let Some(name) = args.get("bug") {
        match InjectedBug::parse(name) {
            Some(b) => cfg.raft.bug = b,
            None => {
                let names: Vec<&str> = InjectedBug::ALL.iter().map(|b| b.name()).collect();
                eprintln!("error: unknown --bug {name:?}; try one of: {}", names.join(", "));
                std::process::exit(2);
            }
        }
    }
    if args.has("quorum-loss") {
        // Let the injector take down a majority. Safety must still hold;
        // progress plainly cannot, so the liveness check stands down.
        cfg.faults.allow_quorum_loss = true;
        cfg.check_liveness = false;
    }
    if let Some(n) = args.opt_u64("max-events") {
        cfg.max_events = n;
    }
    if let Some(n) = args.opt_u64("check-durability-every") {
        cfg.check_durability_every = n.max(1);
    }
    cfg.normalise();
    cfg
}

// ---- argument parsing ---------------------------------------------------

#[derive(Debug)]
struct Args {
    flags: BTreeMap<String, String>,
}

impl Args {
    fn parse(argv: &[String]) -> Result<Args, String> {
        let mut flags = BTreeMap::new();
        let mut i = 0;
        while i < argv.len() {
            let arg = &argv[i];
            let Some(name) = arg.strip_prefix("--") else {
                return Err(format!("unexpected argument {arg:?}"));
            };
            if let Some((k, v)) = name.split_once('=') {
                flags.insert(k.to_string(), v.to_string());
                i += 1;
                continue;
            }
            // A flag is a boolean unless the next token is a value.
            let takes_value = argv
                .get(i + 1)
                .is_some_and(|next| !next.starts_with("--"));
            if takes_value {
                flags.insert(name.to_string(), argv[i + 1].clone());
                i += 2;
            } else {
                flags.insert(name.to_string(), "true".to_string());
                i += 1;
            }
        }
        Ok(Args { flags })
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.flags.get(key).map(String::as_str)
    }

    fn has(&self, key: &str) -> bool {
        self.flags.contains_key(key)
    }

    fn opt_u64(&self, key: &str) -> Option<u64> {
        let raw = self.get(key)?;
        match raw.replace('_', "").parse() {
            Ok(v) => Some(v),
            Err(_) => {
                eprintln!("error: --{key} expects a number, got {raw:?}");
                std::process::exit(2);
            }
        }
    }

    fn u64(&self, key: &str, default: u64) -> u64 {
        self.opt_u64(key).unwrap_or(default)
    }

    fn usize(&self, key: &str, default: usize) -> usize {
        self.opt_u64(key).map_or(default, |v| v as usize)
    }

    fn level(&self) -> Option<Level> {
        let raw = self.get("trace")?;
        match Level::parse(raw) {
            Some(l) => Some(l),
            None => {
                eprintln!("error: --trace expects off|error|warn|info|debug, got {raw:?}");
                std::process::exit(2);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Args {
        Args::parse(&list.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap()
    }

    #[test]
    fn parses_values_and_booleans() {
        let a = args(&["--seed", "42", "--benign", "--threads=8"]);
        assert_eq!(a.u64("seed", 0), 42);
        assert!(a.has("benign"));
        assert_eq!(a.usize("threads", 1), 8);
        assert!(!a.has("verbose"));
        assert_eq!(a.u64("missing", 7), 7);
    }

    #[test]
    fn a_trailing_boolean_flag_is_not_eaten_by_the_next_flag() {
        let a = args(&["--verbose", "--seeds", "10"]);
        assert!(a.has("verbose"));
        assert_eq!(a.u64("seeds", 0), 10);
    }

    #[test]
    fn stray_positional_arguments_are_rejected() {
        let e = Args::parse(&["oops".to_string()]).unwrap_err();
        assert!(e.contains("oops"));
    }

    #[test]
    fn durations_are_read_in_milliseconds() {
        let cfg = config(&args(&["--duration", "1500", "--settle", "2000"]));
        assert_eq!(cfg.duration, 1500 * MILLIS);
        assert_eq!(cfg.settle, 2000 * MILLIS);
        // The fault window always tracks the run length.
        assert_eq!(cfg.faults.window.1, cfg.duration);
    }

    #[test]
    fn benign_disables_every_fault() {
        let cfg = config(&args(&["--benign", "--seed", "3"]));
        assert!(!cfg.faults.any_enabled());
        assert_eq!(cfg.net.drop_ppm, 0);
        assert_eq!(cfg.disk.torn_write_ppm, 0);
        assert_eq!(cfg.seed, 3);
    }

    #[test]
    fn trace_levels_parse() {
        assert_eq!(args(&["--trace", "debug"]).level(), Some(Level::Debug));
        assert_eq!(args(&[]).level(), None);
    }
}
