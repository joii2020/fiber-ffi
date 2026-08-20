use std::{
    env,
    error::Error,
    fs::OpenOptions,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{mpsc, Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use ckb_sdk::Address;
use fiber_ffi::native::{
    self,
    types::{
        ListChannelsParams, Multiaddr, NewInvoiceParams, OpenChannelParams, Pubkey,
        SendPaymentCommandParams, ShutdownChannelParams, TransportType,
    },
    CkbHistoryDiscoveryOptions, ConnectPeerOptions, FiberNode, StartOptions,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};

const INPUT_SIZE: usize = 8192;
const DEFAULT_CHANNEL_FUNDING_SHANNONS: u128 = 50_000_000_000;
const DEFAULT_CKB_DISCOVERY_RPC_URL: &str = "https://testnet.ckbapp.dev/";
const DEFAULT_CKB_HISTORY_SAFETY_BLOCKS: u64 = 1_000;
const DEFAULT_CKB_MAX_INDEXER_LAG: u64 = 100;

type AnyResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug)]
struct CliArgs {
    config: PathBuf,
    data: PathBuf,
    log_level: String,
    log_file: Option<PathBuf>,
    ckb_discovery_rpc: String,
}

impl Default for CliArgs {
    fn default() -> Self {
        Self {
            config: PathBuf::from("tools/fiber-demo-cli/config.testnet.yml"),
            data: PathBuf::from("tools/fiber-demo-cli/data"),
            log_level: "info,fiber_ffi=debug".to_string(),
            log_file: None,
            ckb_discovery_rpc: DEFAULT_CKB_DISCOVERY_RPC_URL.to_string(),
        }
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("fiber-demo-cli: {error}");
        std::process::exit(1);
    }
}

fn run() -> AnyResult<()> {
    let Some(mut args) = parse_args(env::args())? else {
        return Ok(());
    };
    if args.log_file.is_none() {
        args.log_file = Some(args.data.join("fiber-ffi.log"));
    }
    validate_startup(&args)?;

    let log_file = args.log_file.as_ref().expect("default log path was set");
    let event_log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file)
        .map_err(|error| format!("unable to open log file {}: {error}", log_file.display()))?;
    env::set_var("FIBER_FFI_LOG_FILE", log_file);

    println!(
        "Fiber FFI Rust Demo CLI\n  Config:   {}\n  Data:     {}\n  Log:      {}\n  Initial wallet discovery RPC: {}\n  Version:  {}",
        args.config.display(),
        args.data.display(),
        log_file.display(),
        args.ckb_discovery_rpc,
        native::version(),
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut start_options = StartOptions::new(path_text(&args.config));
    start_options.database_prefix = Some(path_text(&args.data));
    start_options.log_level = args.log_level.clone();

    println!("\n[startup/0] Determining the wallet history start block...");
    let history_start_block =
        discover_initial_history_start_block(&runtime, &args, &start_options)?;

    println!("[startup/1] Synchronizing the built-in CKB Light Client...");
    prepare_ckb(&start_options, history_start_block)?;

    let event_log = Arc::new(Mutex::new(event_log));
    start_options.event_handler = Some(Arc::new(move |event| {
        let Ok(mut file) = event_log.lock() else {
            return;
        };
        match serde_json::to_string(&native::event_to_json(event)) {
            Ok(event) => {
                let _ = writeln!(file, "[event] {event}");
            }
            Err(error) => {
                let _ = writeln!(file, "[event/error] {error}");
            }
        }
    }));

    println!("[startup/2] Initializing and starting Fiber...");
    let node = FiberNode::start(start_options)?;
    println!("[startup/2] Fiber started successfully");

    let menu_result = runtime.block_on(async {
        match node.node_info().await {
            Ok(info) => print_json("node-info", &native::node_info_to_json(info)),
            Err(error) => print_native_error("node-info", &error),
        }

        println!(
            "[startup/3] Querying the funding wallet balance through the CKB Light Client..."
        );
        match node.ckb_balance().await {
            Ok(balance) => {
                print_json("ckb/wallet-balance", &balance);
                println!(
                    "[startup/3] When opening a channel, reserve capacity for a change Cell and transaction fees."
                );
            }
            Err(error) => {
                print_native_error("ckb/wallet-balance", &error);
                eprintln!(
                    "[startup/3] Balance query failed. Fiber is running, but verify the wallet balance before opening a channel."
                );
            }
        }

        menu_loop(&node).await
    });

    println!("\n[shutdown] Stopping Fiber...");
    let stop_result = node.stop();
    match stop_result {
        Ok(()) => println!("[shutdown] Fiber stopped"),
        Err(ref error) => print_native_error("shutdown", error),
    }
    menu_result?;
    stop_result?;
    Ok(())
}

fn parse_args(arguments: impl IntoIterator<Item = String>) -> Result<Option<CliArgs>, String> {
    let mut args = CliArgs::default();
    let mut arguments = arguments.into_iter();
    let program = arguments
        .next()
        .unwrap_or_else(|| "fiber-demo-cli".to_string());

    while let Some(argument) = arguments.next() {
        if argument == "-h" || argument == "--help" {
            print_help(&program);
            return Ok(None);
        }
        let value = arguments
            .next()
            .ok_or_else(|| format!("{argument} requires a value"))?;
        match argument.as_str() {
            "--config" => args.config = PathBuf::from(value),
            "--data" => args.data = PathBuf::from(value),
            "--log-level" => args.log_level = value,
            "--log-file" => args.log_file = Some(PathBuf::from(value)),
            "--ckb-discovery-rpc" => args.ckb_discovery_rpc = value,
            _ => {
                return Err(format!(
                    "unknown argument: {argument} (use --help for usage)"
                ))
            }
        }
    }
    Ok(Some(args))
}

fn print_help(program: &str) {
    println!(
        "fiber-ffi Rust demo CLI (rlib)\n\nUsage:\n  {program} [options]\n\nOptions:\n  --config PATH     Path to the Fiber YAML configuration\n  --data PATH       Fiber/CKB Light Client data directory\n  --log-level TEXT  fiber-ffi log filter\n  --log-file PATH   Log file [default: <data>/fiber-ffi.log]\n  --ckb-discovery-rpc URL\n                    CKB RPC/Indexer used for initial wallet history discovery\n  -h, --help        Show this help"
    );
}

fn validate_startup(args: &CliArgs) -> AnyResult<()> {
    if !args.config.is_file() {
        return Err(format!("configuration file not found: {}", args.config.display()).into());
    }
    let key_path = args.data.join("ckb/key");
    if !key_path.is_file() {
        return Err(format!(
            "CKB wallet private key not found at {}; fiber-demo-cli cannot be used without a funded wallet. Place the private key for a funded CKB testnet wallet at this path and try again",
            key_path.display()
        )
        .into());
    }
    if env::var_os("FIBER_SECRET_KEY_PASSWORD").is_none() {
        return Err(
            "FIBER_SECRET_KEY_PASSWORD is not set; use `make -C tools/fiber-demo-cli run` or set it manually"
                .into(),
        );
    }
    Ok(())
}

fn discover_initial_history_start_block(
    runtime: &tokio::runtime::Runtime,
    args: &CliArgs,
    options: &StartOptions,
) -> AnyResult<Option<u64>> {
    let birthday_path = args.data.join("ckb/wallet-birthday.json");
    if birthday_path.is_file() {
        println!(
            "[startup/0] Using persisted wallet birthday: {}",
            birthday_path.display()
        );
        return Ok(None);
    }

    let funding_address =
        native::ckb_funding_address(&options.config_path, options.database_prefix.clone())?;
    let address = funding_address
        .parse::<Address>()
        .map_err(|error| format!("invalid configured funding address: {error}"))?;
    let funding_lock = address.payload().into();
    let history_start_block = runtime.block_on(native::discover_ckb_history_start_block(
        CkbHistoryDiscoveryOptions {
            rpc_url: args.ckb_discovery_rpc.clone(),
            funding_lock,
            safety_blocks: DEFAULT_CKB_HISTORY_SAFETY_BLOCKS,
            max_indexer_lag: DEFAULT_CKB_MAX_INDEXER_LAG,
        },
    ))?;

    println!(
        "[startup/0] Funding address: {funding_address}\n[startup/0] External RPC: {}\n[startup/0] Suggested history start block: {history_start_block} (0x{history_start_block:x})",
        args.ckb_discovery_rpc
    );
    Ok(Some(history_start_block))
}

fn prepare_ckb(options: &StartOptions, history_start_block: Option<u64>) -> AnyResult<()> {
    let (sender, receiver) = mpsc::channel();
    let started_at = Instant::now();
    native::prepare_ckb(
        options,
        history_start_block,
        Arc::new(move |result| match result {
            Ok(value) if value.get("ready").and_then(Value::as_bool) != Some(true) => {
                println!("[startup/1] CKB Light Client status: {value}");
            }
            result => {
                let _ = sender.send(result);
            }
        }),
    )?;

    let result = receiver
        .recv()
        .map_err(|error| format!("CKB preparation callback stopped unexpectedly: {error}"))??;
    let ready = result.get("ready").and_then(Value::as_bool) == Some(true)
        && result.get("mode").and_then(Value::as_str) == Some("light_client");
    if !ready {
        return Err(format!(
            "the rlib was not built with the embedded CKB Light Client; prepare result: {result}"
        )
        .into());
    }
    println!(
        "[startup/1] CKB Light Client is ready (elapsed: {:.3} seconds): {result}",
        started_at.elapsed().as_secs_f64()
    );
    Ok(())
}

async fn menu_loop(node: &FiberNode) -> io::Result<()> {
    loop {
        print_current_time();
        println!("\n========== Fiber Main Menu ==========\n1. Peer\n2. Channel\n3. Pay\nq. Exit");
        let Some(choice) = prompt_line("Select")? else {
            return Ok(());
        };
        match choice.as_str() {
            "1" => {
                if !peer_menu(node).await? {
                    return Ok(());
                }
            }
            "2" => {
                if !channel_menu(node).await? {
                    return Ok(());
                }
            }
            "3" => {
                if !pay_menu(node).await? {
                    return Ok(());
                }
            }
            "q" => return Ok(()),
            _ => println!("[input] Invalid menu choice"),
        }
    }
}

async fn peer_menu(node: &FiberNode) -> io::Result<bool> {
    loop {
        print_current_time();
        println!("\n---------- Peer Menu ----------\n1. Connect\n2. Disconnect\n3. List\n0. Back");
        let Some(choice) = prompt_line("Select")? else {
            return Ok(false);
        };
        match choice.as_str() {
            "1" => {
                if !peer_connect(node).await? {
                    return Ok(false);
                }
            }
            "2" => {
                if !peer_disconnect(node).await? {
                    return Ok(false);
                }
            }
            "3" => print_json_result("peer/list", node.list_peers().await),
            "0" => return Ok(true),
            _ => println!("[input] Invalid menu choice"),
        }
    }
}

async fn peer_connect(node: &FiberNode) -> io::Result<bool> {
    println!("\nConnection method:\n1. Full multiaddr (recommended)\n2. Peer public key");
    let Some(mode) = prompt_line("Select")? else {
        return Ok(false);
    };
    let options = match mode.as_str() {
        "1" => {
            let Some(address) = prompt_required("multiaddr")? else {
                return Ok(false);
            };
            let Some(save) = prompt_yes_no("Save peer address", true)? else {
                return Ok(false);
            };
            match address.parse::<Multiaddr>() {
                Ok(address) => ConnectPeerOptions::Address { address, save },
                Err(error) => {
                    println!("[peer/connect/error] Invalid multiaddr: {error}");
                    return Ok(true);
                }
            }
        }
        "2" => {
            let Some(pubkey) = prompt_required("Peer public key")? else {
                return Ok(false);
            };
            let Some(address_type) = prompt_line("Connection type [tcp/ws/wss]")? else {
                return Ok(false);
            };
            let pubkey = match parse_pubkey(&pubkey) {
                Ok(pubkey) => pubkey,
                Err(error) => {
                    println!("[peer/connect/error] {error}");
                    return Ok(true);
                }
            };
            let address_type = match parse_transport(&address_type) {
                Ok(address_type) => address_type,
                Err(error) => {
                    println!("[peer/connect/error] {error}");
                    return Ok(true);
                }
            };
            ConnectPeerOptions::Pubkey {
                pubkey,
                address_type,
            }
        }
        _ => {
            println!("[input] Invalid connection method");
            return Ok(true);
        }
    };

    print_unit_result("peer/connect", node.connect_peer(options).await);
    Ok(true)
}

async fn peer_disconnect(node: &FiberNode) -> io::Result<bool> {
    let Some(pubkey) = prompt_required("Peer public key")? else {
        return Ok(false);
    };
    match parse_pubkey(&pubkey) {
        Ok(pubkey) => print_unit_result("peer/disconnect", node.disconnect_peer(pubkey).await),
        Err(error) => println!("[peer/disconnect/error] {error}"),
    }
    Ok(true)
}

async fn channel_menu(node: &FiberNode) -> io::Result<bool> {
    loop {
        print_current_time();
        println!(
            "\n---------- Channel Menu ----------\n1. Open\n2. Close\n3. List (all except failed history)\n0. Back"
        );
        let Some(choice) = prompt_line("Select")? else {
            return Ok(false);
        };
        match choice.as_str() {
            "1" => {
                if !channel_open(node).await? {
                    return Ok(false);
                }
            }
            "2" => {
                if !channel_close(node).await? {
                    return Ok(false);
                }
            }
            "3" => {
                let params = ListChannelsParams {
                    pubkey: None,
                    include_closed: Some(true),
                    only_pending: None,
                };
                print_json_result("channel/list", node.list_channels(params).await);
            }
            "0" => return Ok(true),
            _ => println!("[input] Invalid menu choice"),
        }
    }
}

async fn channel_open(node: &FiberNode) -> io::Result<bool> {
    let Some(pubkey) = prompt_required("Remote peer public key")? else {
        return Ok(false);
    };
    let Some(amount) =
        prompt_optional_u128("Funding amount (shannons; blank defaults to 500 CKB)")?
    else {
        return Ok(false);
    };
    let amount = amount.unwrap_or_else(|| {
        println!("[input] Using the default funding amount: 500 CKB (50000000000 shannons)");
        DEFAULT_CHANNEL_FUNDING_SHANNONS
    });
    if amount == 0 {
        println!("[channel/open/error] Funding amount must be greater than 0");
        return Ok(true);
    }
    let Some(public) =
        prompt_optional_bool("Announce channel publicly [y/n; blank uses the Fiber default]")?
    else {
        return Ok(false);
    };
    let Some(one_way) =
        prompt_optional_bool("Funded by one side only [y/n; blank uses the Fiber default]")?
    else {
        return Ok(false);
    };
    let Some(udt_json) = prompt_line("UDT type script JSON [leave blank for a CKB channel]")?
    else {
        return Ok(false);
    };
    let Some(funding_fee_rate) =
        prompt_optional_u64("Funding fee rate [blank uses the Fiber default]")?
    else {
        return Ok(false);
    };

    let mut object = Map::new();
    object.insert("pubkey".to_string(), Value::String(pubkey));
    object.insert(
        "funding_amount".to_string(),
        Value::String(format!("0x{amount:x}")),
    );
    insert_optional(&mut object, "public", public.map(Value::Bool));
    insert_optional(&mut object, "one_way", one_way.map(Value::Bool));
    if !udt_json.is_empty() {
        match serde_json::from_str(&udt_json) {
            Ok(value) => {
                object.insert("funding_udt_type_script".to_string(), value);
            }
            Err(error) => {
                println!("[channel/open/error] Invalid UDT type script JSON: {error}");
                return Ok(true);
            }
        }
    }
    insert_optional(
        &mut object,
        "funding_fee_rate",
        funding_fee_rate.map(|value| Value::String(format!("0x{value:x}"))),
    );
    match decode_params::<OpenChannelParams>(object) {
        Ok(params) => print_json_result(
            "channel/open temporary_channel_id",
            node.open_channel(params).await,
        ),
        Err(error) => println!("[channel/open/error] Invalid options: {error}"),
    }
    Ok(true)
}

async fn channel_close(node: &FiberNode) -> io::Result<bool> {
    let Some(channel_id) = prompt_required("channel_id")? else {
        return Ok(false);
    };
    let Some(force) = prompt_yes_no("Force close", false)? else {
        return Ok(false);
    };
    let Some(close_script) = prompt_line("Close script JSON [blank uses the default]")? else {
        return Ok(false);
    };
    let Some(fee_rate) = prompt_optional_u64("Fee rate [blank uses the default]")? else {
        return Ok(false);
    };

    let mut object = Map::new();
    object.insert("channel_id".to_string(), Value::String(channel_id));
    object.insert("force".to_string(), Value::Bool(force));
    if !close_script.is_empty() {
        match serde_json::from_str(&close_script) {
            Ok(value) => {
                object.insert("close_script".to_string(), value);
            }
            Err(error) => {
                println!("[channel/close/error] Invalid close script JSON: {error}");
                return Ok(true);
            }
        }
    }
    insert_optional(
        &mut object,
        "fee_rate",
        fee_rate.map(|value| Value::String(format!("0x{value:x}"))),
    );
    match decode_params::<ShutdownChannelParams>(object) {
        Ok(params) => print_unit_result("channel/close", node.shutdown_channel(params).await),
        Err(error) => println!("[channel/close/error] Invalid options: {error}"),
    }
    Ok(true)
}

async fn pay_menu(node: &FiberNode) -> io::Result<bool> {
    loop {
        print_current_time();
        println!("\n---------- Pay Menu ----------\n1. Create invoice\n2. Pay invoice\n0. Back");
        let Some(choice) = prompt_line("Select")? else {
            return Ok(false);
        };
        match choice.as_str() {
            "1" => {
                if !pay_create_invoice(node).await? {
                    return Ok(false);
                }
            }
            "2" => {
                if !pay_invoice(node).await? {
                    return Ok(false);
                }
            }
            "0" => return Ok(true),
            _ => println!("[input] Invalid menu choice"),
        }
    }
}

async fn pay_create_invoice(node: &FiberNode) -> io::Result<bool> {
    let Some(amount) = prompt_optional_u128("Invoice amount (shannons)")? else {
        return Ok(false);
    };
    let Some(amount) = amount.filter(|amount| *amount > 0) else {
        println!("[pay/invoice/error] Invoice amount must be greater than 0");
        return Ok(true);
    };
    let Some(description) = prompt_line("Description [optional]")? else {
        return Ok(false);
    };
    println!("Currency: 1. Fibb/mainnet CKB  2. Fibt/testnet CKB  3. Fibd/UDT");
    let Some(currency) = prompt_line("Select currency")? else {
        return Ok(false);
    };
    let currency = match currency.as_str() {
        "1" => "Fibb",
        "" | "2" => "Fibt",
        "3" => "Fibd",
        _ => {
            println!("[pay/invoice/error] Invalid currency");
            return Ok(true);
        }
    };
    let udt_json = if currency == "Fibd" {
        let Some(value) = prompt_line("UDT type script JSON")? else {
            return Ok(false);
        };
        value
    } else {
        String::new()
    };
    let Some(expiry) = prompt_optional_u64("Expiry in seconds [blank uses the Fiber default]")?
    else {
        return Ok(false);
    };
    let Some(allow_mpp) =
        prompt_optional_bool("Allow multi-path payments (MPP) [y/n; blank uses the default]")?
    else {
        return Ok(false);
    };

    let mut object = Map::new();
    object.insert("amount".to_string(), Value::String(format!("0x{amount:x}")));
    object.insert("currency".to_string(), Value::String(currency.to_string()));
    if !description.is_empty() {
        object.insert("description".to_string(), Value::String(description));
    }
    if !udt_json.is_empty() {
        match serde_json::from_str(&udt_json) {
            Ok(value) => {
                object.insert("udt_type_script".to_string(), value);
            }
            Err(error) => {
                println!("[pay/invoice/error] Invalid UDT type script JSON: {error}");
                return Ok(true);
            }
        }
    }
    insert_optional(
        &mut object,
        "expiry",
        expiry.map(|value| Value::String(format!("0x{value:x}"))),
    );
    insert_optional(&mut object, "allow_mpp", allow_mpp.map(Value::Bool));
    match decode_params::<NewInvoiceParams>(object) {
        Ok(params) => print_json_result("pay/invoice", node.new_invoice(params).await),
        Err(error) => println!("[pay/invoice/error] Invalid options: {error}"),
    }
    Ok(true)
}

async fn pay_invoice(node: &FiberNode) -> io::Result<bool> {
    let Some(invoice) = prompt_required("invoice")? else {
        return Ok(false);
    };
    let Some(timeout) = prompt_optional_u64("Timeout in seconds [blank uses the Fiber default]")?
    else {
        return Ok(false);
    };
    let Some(max_fee_amount) =
        prompt_optional_u128("Maximum fee in shannons [blank uses the Fiber default]")?
    else {
        return Ok(false);
    };
    let Some(dry_run) = prompt_yes_no("Dry run only (do not send payment)", false)? else {
        return Ok(false);
    };

    let mut object = Map::new();
    object.insert("invoice".to_string(), Value::String(invoice));
    object.insert("dry_run".to_string(), Value::Bool(dry_run));
    insert_optional(
        &mut object,
        "timeout",
        timeout.map(|value| Value::String(format!("0x{value:x}"))),
    );
    insert_optional(
        &mut object,
        "max_fee_amount",
        max_fee_amount.map(|value| Value::String(format!("0x{value:x}"))),
    );
    match decode_params::<SendPaymentCommandParams>(object) {
        Ok(params) => print_json_result("pay/send", node.send_payment(params).await),
        Err(error) => println!("[pay/send/error] Invalid options: {error}"),
    }
    Ok(true)
}

fn prompt_line(label: &str) -> io::Result<Option<String>> {
    loop {
        print!("{label}> ");
        io::stdout().flush()?;
        let mut input = String::new();
        if io::stdin().read_line(&mut input)? == 0 {
            return Ok(None);
        }
        while input.ends_with(['\n', '\r']) {
            input.pop();
        }
        let input = input.trim().to_string();
        if input.len() <= INPUT_SIZE - 2 {
            return Ok(Some(input));
        }
        println!(
            "[input] Input is too long (maximum: {} bytes)",
            INPUT_SIZE - 2
        );
    }
}

fn prompt_required(label: &str) -> io::Result<Option<String>> {
    loop {
        let Some(input) = prompt_line(label)? else {
            return Ok(None);
        };
        if !input.is_empty() {
            return Ok(Some(input));
        }
        println!("[input] This field is required");
    }
}

fn prompt_yes_no(label: &str, default: bool) -> io::Result<Option<bool>> {
    let suffix = if default { "[Y/n]" } else { "[y/N]" };
    loop {
        let Some(input) = prompt_line(&format!("{label} {suffix}"))? else {
            return Ok(None);
        };
        match input.to_ascii_lowercase().as_str() {
            "" => return Ok(Some(default)),
            "y" | "yes" => return Ok(Some(true)),
            "n" | "no" => return Ok(Some(false)),
            _ => println!("[input] Please enter y or n"),
        }
    }
}

fn prompt_optional_bool(label: &str) -> io::Result<Option<Option<bool>>> {
    loop {
        let Some(input) = prompt_line(label)? else {
            return Ok(None);
        };
        match input.to_ascii_lowercase().as_str() {
            "" => return Ok(Some(None)),
            "y" | "yes" => return Ok(Some(Some(true))),
            "n" | "no" => return Ok(Some(Some(false))),
            _ => println!("[input] Please enter y, n, or leave the field blank"),
        }
    }
}

fn prompt_optional_u64(label: &str) -> io::Result<Option<Option<u64>>> {
    prompt_optional_number(label, "a valid non-negative integer")
}

fn prompt_optional_u128(label: &str) -> io::Result<Option<Option<u128>>> {
    prompt_optional_number(label, "a valid non-negative integer (maximum: 2^128-1)")
}

fn prompt_optional_number<T>(label: &str, expected: &str) -> io::Result<Option<Option<T>>>
where
    T: std::str::FromStr,
{
    loop {
        let Some(input) = prompt_line(label)? else {
            return Ok(None);
        };
        if input.is_empty() {
            return Ok(Some(None));
        }
        if input.bytes().all(|byte| byte.is_ascii_digit()) {
            if let Ok(value) = input.parse::<T>() {
                return Ok(Some(Some(value)));
            }
        }
        println!("[input] Please enter {expected}");
    }
}

fn parse_pubkey(value: &str) -> Result<Pubkey, String> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    let bytes = hex::decode(value).map_err(|error| format!("Invalid public key hex: {error}"))?;
    if bytes.len() != Pubkey::serialization_len() {
        return Err(format!(
            "Public key must be a {}-byte compressed secp256k1 key",
            Pubkey::serialization_len()
        ));
    }
    Pubkey::from_slice(&bytes).map_err(|error| format!("Invalid public key: {error}"))
}

fn parse_transport(value: &str) -> Result<Option<TransportType>, String> {
    match value.to_ascii_lowercase().as_str() {
        "" | "tcp" => Ok(Some(TransportType::Tcp)),
        "ws" => Ok(Some(TransportType::Ws)),
        "wss" => Ok(Some(TransportType::Wss)),
        _ => Err("Connection type must be tcp, ws, or wss".to_string()),
    }
}

fn decode_params<T: DeserializeOwned>(object: Map<String, Value>) -> serde_json::Result<T> {
    serde_json::from_value(Value::Object(object))
}

fn insert_optional(object: &mut Map<String, Value>, name: &str, value: Option<Value>) {
    if let Some(value) = value {
        object.insert(name.to_string(), value);
    }
}

fn print_json_result<T: Serialize>(label: &str, result: native::Result<T>) {
    match result {
        Ok(value) => print_json(label, &value),
        Err(error) => print_native_error(label, &error),
    }
}

fn print_unit_result(label: &str, result: native::Result<()>) {
    match result {
        Ok(()) => println!("[{label}/ok] operation submitted"),
        Err(error) => print_native_error(label, &error),
    }
}

fn print_json<T: Serialize>(label: &str, value: &T) {
    match serde_json::to_string_pretty(value) {
        Ok(value) => println!("[{label}/ok]\n{value}"),
        Err(error) => println!("[{label}/error] Unable to serialize result: {error}"),
    }
}

fn print_native_error(label: &str, error: &native::Error) {
    eprintln!("[{label}/error] {:?}: {}", error.kind(), error.message());
}

fn print_current_time() {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    println!("[current time] {}", format_utc_timestamp(seconds));
}

fn format_utc_timestamp(seconds: u64) -> String {
    let days = (seconds / 86_400) as i64;
    let seconds_in_day = seconds % 86_400;
    let hour = seconds_in_day / 3_600;
    let minute = seconds_in_day % 3_600 / 60;
    let second = seconds_in_day % 60;

    // Gregorian civil date conversion from a day count relative to 1970-01-01.
    let shifted_days = days + 719_468;
    let era = if shifted_days >= 0 {
        shifted_days
    } else {
        shifted_days - 146_096
    } / 146_097;
    let day_of_era = shifted_days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = month_index + if month_index < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);

    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02} UTC")
}

fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cli_options() {
        let args = parse_args([
            "fiber-demo-cli".to_string(),
            "--data".to_string(),
            "/tmp/fiber".to_string(),
            "--ckb-discovery-rpc".to_string(),
            "http://127.0.0.1:8114".to_string(),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(args.data, PathBuf::from("/tmp/fiber"));
        assert_eq!(args.ckb_discovery_rpc, "http://127.0.0.1:8114");
    }

    #[test]
    fn decodes_the_channel_list_options_used_by_the_menu() {
        let params: ListChannelsParams = serde_json::from_value(serde_json::json!({
            "include_closed": true
        }))
        .unwrap();
        assert_eq!(params.include_closed, Some(true));
        assert_eq!(params.only_pending, None);
    }

    #[test]
    fn validates_peer_transport_names() {
        assert_eq!(parse_transport("").unwrap(), Some(TransportType::Tcp));
        assert_eq!(parse_transport("WSS").unwrap(), Some(TransportType::Wss));
        assert!(parse_transport("quic").is_err());
    }

    #[test]
    fn formats_current_time_for_humans() {
        assert_eq!(format_utc_timestamp(0), "1970-01-01 00:00:00 UTC");
        assert_eq!(
            format_utc_timestamp(1_777_123_519),
            "2026-04-25 13:25:19 UTC"
        );
    }
}
