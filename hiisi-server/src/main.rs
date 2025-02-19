use clap::Parser;
use socket2::{Domain, SockAddr, Socket, Type};

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use std::path::PathBuf;

use ctrlc;
use hiisi::{Context, HiisiError, ResourceManager, Result, IO};

#[derive(Parser)]
#[command(name = "Hiisi")]
struct Cli {
    #[clap(long, short, default_value = "data", env = "SQLD_DB_PATH")]
    db_path: PathBuf,

    #[arg(long, default_value = "127.0.0.1:8080", env = "SQLD_HTTP_LISTEN_ADDR")]
    http_listen_addr: SocketAddr,
}

fn main() {
    init_logger();
    let cli = Cli::parse();
    if let Err(e) = server_loop(cli) {
        log::error!("Error: {}", e);
        std::process::exit(1);
    }
}

fn server_loop(cli: Cli) -> Result<()> {
    log::info!("Listening for HTTP requests on {:?}", cli.http_listen_addr);

    let listen_addr: SockAddr = cli.http_listen_addr.into();
    let sock = listen(&listen_addr)?;

    let manager = Rc::new(ResourceManager::new(&cli.db_path));
    let ctx = Context::<()>::new(manager, ());
    let mut io = IO::new(ctx);

    let running = Arc::new(AtomicBool::new(true));
    ctrlc::set_handler({
        let running = running.clone();
        move || {
            print!("Received SIGINT, shutting down...\n");
            running.store(false, Ordering::SeqCst);
        }
    })
    .unwrap();
    hiisi::serve(&mut io, sock, listen_addr);
    while running.load(Ordering::SeqCst) {
        io.run_once();
    }
    Ok(())
}

fn listen(addr: &SockAddr) -> Result<Rc<Socket>> {
    let sock = Rc::new(
        Socket::new(Domain::IPV4, Type::STREAM, None)
            .map_err(|e| HiisiError::IOError("socket", e))?,
    );
    sock.bind(addr)
        .map_err(|e| HiisiError::IOError("bind", e))?;
    sock.set_reuse_address(true)
        .map_err(|e| HiisiError::IOError("set_reuse_address", e))?;
    sock.set_reuse_port(true)
        .map_err(|e| HiisiError::IOError("set_reuse_port", e))?;
    sock.listen(128)
        .map_err(|e| HiisiError::IOError("listern", e))?;
    Ok(sock)
}

fn init_logger() {
    let env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(env).init();
}
