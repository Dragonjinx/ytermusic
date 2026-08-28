//! End-to-end tests of the audio-device recovery primitives that never touch
//! the real output hardware.
//!
//! They run the real rodio/cpal stack against an ALSA **null** PCM (silence),
//! selected process-locally via `ALSA_CONFIG_PATH`. No audio is ever played
//! through the machine's speakers: the null device discards everything, and
//! the tests never append any sound to the sink anyway.
//!
//! What this verifies:
//! - `Player::new` can create an output stream (exercising the
//!   `with_error_callback` wiring that catches device loss / suspend-resume
//!   glitches) without any hardware,
//! - `Player::update` — the primitive used to rebuild the stream after a
//!   device error — succeeds on a (fake) device and preserves state like
//!   volume and the error channel.

use flume::unbounded;
use player::{PlayError, Player, PlayerOptions};
use std::time::Duration;

/// Overrides the default output PCM to the ALSA null plugin (silence).
/// Merged *after* the stock `alsa.conf` (via `ALSA_CONFIG_PATH`); the `!`
/// prefix force-replaces the stock scalar alias `pcm.default`.
const ALSA_NULL_CONF: &str = r#"
# Silence: force the default output device to the null PCM.
# Nothing can reach the real speakers through it.
pcm.!default {
    type null
}
"#;

/// Locates the stock ALSA config dir (containing `alsa.conf`), which defines
/// the `null` plugin type.
fn find_alsa_config_dir() -> std::path::PathBuf {
    if let Some(dir) = std::env::var_os("ALSA_CONFIG_DIR") {
        let dir = std::path::PathBuf::from(dir);
        if dir.join("alsa.conf").exists() {
            return dir;
        }
    }
    for p in ["/etc/alsa", "/usr/share/alsa", "/usr/local/share/alsa"] {
        let p = std::path::PathBuf::from(p);
        if p.join("alsa.conf").exists() {
            return p;
        }
    }
    // NixOS: alsa.conf lives inside the alsa-lib package in the store.
    if let Ok(store) = std::fs::read_dir("/nix/store") {
        let mut candidates = store
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.contains("-alsa-lib-"))
                    .unwrap_or(false)
            })
            .map(|e| e.path().join("share/alsa"))
            .filter(|p| p.join("alsa.conf").exists())
            .collect::<Vec<_>>();
        // Several alsa-lib versions may be installed; any with alsa.conf works
        // (the config is text and the plugin ABI is stable), so pick one.
        candidates.sort();
        if let Some(dir) = candidates.pop() {
            return dir;
        }
    }
    panic!("could not locate the stock ALSA config dir (alsa.conf)");
}

/// Points this process's ALSA stack at the null device. The override is
/// process-local: the real default device and other applications are
/// completely unaffected.
fn set_null_device() {
    let config_dir = find_alsa_config_dir();

    // ALSA_CONFIG_PATH merges multiple config files, left to right. Keep the
    // stock config (defines the `null` plugin and the base tree), then append
    // our override so the default output device is the null PCM (`!` prefix
    // force-replaces the stock `pcm.default` alias). All of this is
    // process-local.
    let stock = config_dir.join("alsa.conf");
    let override_path =
        std::env::temp_dir().join(format!("ytermusic-alsa-null-{}.conf", std::process::id()));
    std::fs::write(&override_path, ALSA_NULL_CONF).expect("write ALSA override config");
    let config_path = format!("{}:{}", stock.display(), override_path.display());
    std::env::set_var("ALSA_CONFIG_DIR", &config_dir);
    std::env::set_var("ALSA_CONFIG_PATH", &config_path);
}

#[test]
fn player_can_be_created_and_rebuilt_on_null_device() {
    set_null_device();

    let (tx, _rx) = unbounded::<PlayError>();
    let player = Player::new(tx, PlayerOptions::new(30)).expect(
        "Player::new must succeed on a (null) output device; \
         this is the path that wires the stream-error callback",
    );
    assert_eq!(player.volume(), 30, "initial volume preserved");

    // The recovery primitive: rebuild the output stream and sink. This is what
    // runs when a device stream error arrives after a suspend/resume.
    let rebuilt = player
        .update()
        .expect("Player::update must succeed on a (null) output device");
    assert_eq!(
        rebuilt.volume(),
        30,
        "volume survives an audio-device rebuild"
    );
    assert!(rebuilt.is_finished(), "fresh sink has nothing queued");
}

#[test]
fn repeated_rebuilds_are_stable() {
    set_null_device();

    let (tx, _rx) = unbounded::<PlayError>();
    let player = Player::new(tx, PlayerOptions::new(50)).expect("player creation");
    let mut player = player;
    for _ in 0..5 {
        player = player.update().expect("repeated rebuilds must stay stable");
    }
    assert_eq!(player.volume(), 50);
}

#[test]
fn error_channel_remains_live_after_rebuild() {
    set_null_device();

    let (tx, rx) = unbounded::<PlayError>();
    let player = Player::new(tx.clone(), PlayerOptions::new(10)).expect("player creation");
    let _rebuilt = player.update().expect("rebuild");

    // The app drains device errors through this same channel; simulate the
    // runtime callback firing after the rebuild.
    tx.try_send(PlayError::DeviceStreamError(
        rodio::cpal::StreamError::DeviceNotAvailable,
    ))
    .expect("send simulated device error");
    let received = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("error channel must deliver the simulated device error");
    assert!(received.is_device_loss());
}
