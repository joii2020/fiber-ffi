use fiber_ffi::native::{FiberNode, StartOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = std::env::args()
        .nth(1)
        .ok_or("usage: cargo run --example rust-native -- <config.yml>")?;
    let node = FiberNode::start(StartOptions::new(config_path))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async {
        let info = node.node_info().await?;
        println!("Fiber node {:?} is running", info.node_id);
        Ok::<_, fiber_ffi::native::Error>(())
    })?;
    node.stop()?;
    Ok(())
}
