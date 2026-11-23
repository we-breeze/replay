const TIMER_SOCKET_ENV: &str = "BRZ_REPLAY_TIMER_SOCKET";

fn main() {
    println!("cargo:rerun-if-env-changed={TIMER_SOCKET_ENV}");
    println!("cargo:rustc-check-cfg=cfg(brz_replay_timer)");
    if let Some(path) = std::env::var_os(TIMER_SOCKET_ENV) {
        assert!(
            path.to_str().is_some_and(|path| !path.is_empty()),
            "{TIMER_SOCKET_ENV} must be a nonempty UTF-8 socket path when set at build time"
        );
        println!("cargo:rustc-cfg=brz_replay_timer");
    }
}
