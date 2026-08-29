use std::{
    collections::{HashMap, VecDeque},
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use common_structs::MusicDownloadStatus;
use flume::{unbounded, Receiver, Sender};
use log::{error, info, warn};
use player::{PlayError, Player, PlayerOptions, RecoveryPolicy};

use ytpapi2::YoutubeMusicVideoRef;

use crate::{
    consts::{CACHE_DIR, CONFIG},
    errors::{handle_error, handle_error_option},
    structures::{media::Media, sound_action::SoundAction},
    systems::DOWNLOAD_MANAGER,
    term::{list_selector::ListSelector, playlist::PLAYER_RUNNING, ManagerMessage, Screens},
    DATABASE,
};

/// How long a rebuild worker may take before it is treated as hung (the
/// device is wedged in ALSA/PipeWire stream negotiation). The worker thread
/// itself keeps running and is harmless; we only stop waiting and count the
/// attempt as a failure so the [`RecoveryPolicy`] can surface the DeviceLost
/// screen.
const REBUILD_TIMEOUT: Duration = Duration::from_secs(10);

/// Context captured when a rebuild is requested, used to re-queue the current
/// track at the same position once the rebuilt stream lands.
struct RebuildContext {
    /// Where the (old) sink was when the device died.
    position: Duration,
    /// The track that was current at request time.
    video: Option<YoutubeMusicVideoRef>,
    /// Whether that track was already downloaded at request time.
    downloaded: bool,
    /// Why the rebuild was requested (for logs).
    reason: String,
}

pub struct PlayerState {
    pub goto: Screens,
    pub list: Vec<YoutubeMusicVideoRef>,
    pub current: usize,
    pub rtcurrent: Option<YoutubeMusicVideoRef>,
    pub music_status: HashMap<String, MusicDownloadStatus>,
    pub list_selector: ListSelector,
    pub controls: Media,
    pub sink: Player,
    pub updater: Sender<ManagerMessage>,
    pub soundaction_sender: Sender<SoundAction>,
    pub soundaction_receiver: Receiver<SoundAction>,
    pub stream_error_receiver: Receiver<PlayError>,
    recovery: RecoveryPolicy,
    /// Receiver for the result of an audio-device rebuild worker thread.
    ///
    /// The rebuild opens a new ALSA/PipeWire stream, which can block
    /// indefinitely on a wedged device, so it runs on a worker thread and the
    /// UI thread only ever swaps in the result.
    rebuild_receiver: Receiver<Result<Player, PlayError>>,
    /// True while a rebuild worker is running or its result is pending.
    rebuild_in_flight: bool,
    /// When the in-flight rebuild was started (watchdog; see
    /// [`REBUILD_TIMEOUT`]).
    rebuild_started_at: Option<Instant>,
    /// Track/position captured at request time, applied once the rebuilt
    /// stream lands (re-queue + pause).
    pending_requeue: Option<RebuildContext>,
    last_download_list: Vec<String>,
}

impl PlayerState {
    fn new(
        soundaction_sender: Sender<SoundAction>,
        soundaction_receiver: Receiver<SoundAction>,
        updater: Sender<ManagerMessage>,
    ) -> Self {
        let (stream_error_sender, stream_error_receiver) = unbounded::<PlayError>();
        // A fresh channel is created per rebuild worker; this receiver is only
        // a placeholder until the first rebuild starts.
        let (_rebuild_tx, rebuild_receiver) = unbounded::<Result<Player, PlayError>>();
        let sink = handle_error_option(
            &updater,
            "player creation error",
            Player::new(
                stream_error_sender,
                PlayerOptions::new(CONFIG.player.initial_volume),
            ),
        )
        .unwrap();
        Self {
            controls: Media::new(updater.clone(), soundaction_sender.clone()),
            soundaction_receiver,
            list_selector: ListSelector::default(),
            music_status: HashMap::new(),
            updater,
            stream_error_receiver,
            soundaction_sender,
            sink,
            recovery: RecoveryPolicy::new(),
            rebuild_receiver,
            rebuild_in_flight: false,
            rebuild_started_at: None,
            pending_requeue: None,
            goto: Screens::Playlist,
            list: Vec::new(),
            current: 0,
            rtcurrent: None,
            last_download_list: Vec::new(),
        }
    }

    pub fn current(&self) -> Option<&YoutubeMusicVideoRef> {
        self.relative_current(0)
    }

    pub fn relative_current(&self, n: isize) -> Option<&YoutubeMusicVideoRef> {
        self.list.get(self.current.saturating_add_signed(n))
    }

    pub fn set_relative_current(&mut self, n: isize) {
        self.current = self.current.saturating_add_signed(n);
    }

    pub fn is_current_download_failed(&self) -> bool {
        self.current()
            .as_ref()
            .map(|x| {
                self.music_status.get(&x.video_id) == Some(&MusicDownloadStatus::DownloadFailed)
            })
            .unwrap_or(false)
    }

    pub fn is_current_downloaded(&self) -> bool {
        self.current()
            .as_ref()
            .map(|x| self.music_status.get(&x.video_id) == Some(&MusicDownloadStatus::Downloaded))
            .unwrap_or(false)
    }

    pub fn update(&mut self) {
        // Apply any completed device rebuild first, so the rest of the tick
        // observes the new stream (and its paused state) rather than racing
        // the worker thread.
        self.apply_rebuild_result();
        PLAYER_RUNNING.store(self.current().is_some(), Ordering::SeqCst);
        self.update_controls();
        self.handle_stream_errors();
        // Advance the pre-download spinner so it visibly rotates while yt-dlp
        // is still resolving/negotiating (before any real percentage exists).
        for status in self.music_status.values_mut() {
            if let MusicDownloadStatus::Spinner(frame) = status {
                *frame = frame.wrapping_add(1);
            }
        }
        if self.current > self.list.len() {
            self.current = self.list.len();
        }
        while let Ok(e) = self.soundaction_receiver.try_recv() {
            e.apply_sound_action(self);
        }
        if self.is_current_download_failed() {
            SoundAction::Next(1).apply_sound_action(self);
        }
        if self.sink.is_finished() {
            if self.is_current_downloaded() && self.rtcurrent.as_ref() == self.current() {
                self.set_relative_current(1);
            }
            self.handle_stream_errors();
            self.update_controls();
            // If the current song is finished, we play the next one but if the next one has failed to download, we skip it
            // TODO(optimize this)
            while self
                .current()
                .map(|x| {
                    self.music_status.get(&x.video_id) == Some(&MusicDownloadStatus::DownloadFailed)
                })
                .unwrap_or(false)
            {
                self.set_relative_current(1);
            }

            if self.is_current_downloaded() {
                if let Some(video) = self.current().cloned() {
                    let k = CACHE_DIR.join(format!("downloads/{}.mp4", &video.video_id));
                    if let Err(e) = self.sink.play(k.as_path()) {
                        if matches!(e, PlayError::DecoderError(_)) {
                            // Cleaning the file

                            DATABASE.remove_video(&video);
                            handle_error(
                                &self.updater,
                                "invalid cleaning MP4",
                                std::fs::remove_file(k),
                            );
                            handle_error(
                                &self.updater,
                                "invalid cleaning JSON",
                                std::fs::remove_file(
                                    CACHE_DIR.join(format!("downloads/{}.json", &video.video_id)),
                                ),
                            );
                            self.current = 0;
                            DATABASE.write();
                        } else {
                            self.updater
                                .send(ManagerMessage::PassTo(
                                    Screens::DeviceLost,
                                    Box::new(ManagerMessage::Error(
                                        format!("{e:?}"),
                                        Box::new(None),
                                    )),
                                ))
                                .unwrap();
                        }
                    }
                }
            }
        } else {
            self.rtcurrent = self.current().cloned();
        }
        let to_download = self
            .list
            .iter()
            .skip(self.current)
            .chain(self.list.iter().take(self.current).rev())
            .filter(|x| {
                self.music_status.get(&x.video_id) == Some(&MusicDownloadStatus::NotDownloaded)
            })
            .take(12)
            .cloned()
            .collect::<VecDeque<_>>();
        let new_ids: Vec<String> = to_download.iter().map(|v| v.video_id.clone()).collect();
        if new_ids != self.last_download_list {
            self.last_download_list = new_ids;
            DOWNLOAD_MANAGER.set_download_list(to_download);
        }
    }

    fn handle_stream_errors(&mut self) {
        while let Ok(e) = self.stream_error_receiver.try_recv() {
            error!("Stream error: {:?}", e);
            if !e.is_device_loss() {
                // File/decoder/seek problems are not device failures: surface
                // them as before instead of tearing down the output stream.
                handle_error(&self.updater, "audio device stream error", Err(e));
                continue;
            }
            // The device itself reported a problem (e.g. it got lost or its
            // stream was corrupted by a suspend/resume cycle). Recover by
            // rebuilding the stream and pausing, so the player can't keep
            // driving a broken device or blast the (possibly new) default.
            self.request_recovery("device stream error");
        }
    }

    /// Rebuilds the audio output stream and pauses playback. This is the
    /// single entry point for any device problem (cpal stream error).
    /// repeated failures by surfacing a DeviceLost screen.
    ///
    /// The rebuild itself runs on a worker thread: opening a stream on a
    /// wedged device (ALSA/PipeWire negotiation after suspend/resume) blocks
    /// indefinitely, so doing it here on the UI thread would freeze the TUI.
    fn request_recovery(&mut self, reason: &str) {
        let now = Instant::now();
        if self.recovery.exhausted() {
            handle_error(
                &self.updater,
                "audio device recovery failed",
                Err(PlayError::StreamError(rodio::StreamError::NoDevice)),
            );
            return;
        }
        if self.rebuild_in_flight {
            // A rebuild is already being attempted off the UI thread; the
            // cooldown below plus this guard keep us to one at a time.
            return;
        }
        if !self.recovery.should_attempt(now) {
            // A rebuild was attempted recently; drop this (logged above).
            return;
        }
        self.recovery.record_attempt(now);
        self.start_rebuild(reason);
    }

    /// Manual device rebuild, used by the DeviceLost screen retry
    /// (`RestartPlayer`). Same worker-thread rebuild as the automatic path, but
    /// not gated by the recovery cooldown or exhaustion: the user explicitly
    /// asked to retry.
    pub(crate) fn restart_player(&mut self) {
        if self.rebuild_in_flight {
            info!("Audio-device rebuild already in progress; ignoring manual retry");
            return;
        }
        self.start_rebuild("manual retry");
    }

    /// Captures everything needed to re-queue the current track once the
    /// rebuilt stream lands, then spawns a short-lived worker thread that
    /// re-opens the output device. The UI thread never blocks on the open.
    fn start_rebuild(&mut self, reason: &str) {
        let position = self.sink.elapsed();
        let video = self.current().cloned();
        let downloaded = self.is_current_downloaded();
        let volume = self.sink.volume_percent();
        let error_sender = self.sink.error_sender();

        let (tx, rx) = unbounded::<Result<Player, PlayError>>();
        let thread_name = format!("yt-audio-rebuild-{reason}");
        // Re-open the stream from scratch, like `Player::update()`, but on a
        // worker thread. `Player::update` cannot be used directly: it borrows
        // the live player, which must stay on the UI thread (controls,
        // position and stall detection keep working while the rebuild runs).
        // Re-opening with the current error channel and volume is equivalent
        // for recovery purposes.
        let spawn_result = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let result = Player::new(error_sender, PlayerOptions::new(volume));
                let _ = tx.send(result);
            });
        match spawn_result {
            Ok(_) => {
                self.rebuild_receiver = rx;
                self.rebuild_in_flight = true;
                self.rebuild_started_at = Some(Instant::now());
                self.pending_requeue = Some(RebuildContext {
                    position,
                    video,
                    downloaded,
                    reason: reason.to_owned(),
                });
            }
            Err(e) => {
                error!("Failed to spawn audio-device rebuild thread: {e}");
                self.recovery.record_failure();
                handle_error(
                    &self.updater,
                    "audio device recovery failed",
                    Err(PlayError::StreamError(rodio::StreamError::NoDevice)),
                );
            }
        }
    }

    /// Polls the rebuild worker's result channel and, when a rebuild has
    /// landed, swaps in the new [`Player`], re-queues the current track at its
    /// previous position and pauses. Also enforces the rebuild watchdog. Must
    /// only run on the UI thread: it never opens a device.
    fn apply_rebuild_result(&mut self) {
        if !self.rebuild_in_flight {
            return;
        }

        // Prefer a landed result over the timeout: a slow-but-successful
        // rebuild must not be discarded just because the watchdog elapsed.
        if let Ok(result) = self.rebuild_receiver.try_recv() {
            self.rebuild_in_flight = false;
            self.rebuild_started_at = None;
            let ctx = self.pending_requeue.take();
            match result {
                Ok(player) => {
                    self.sink = player;
                    self.recovery.record_success();
                    info!(
                        "Recovered audio device ({}) and paused; press play to resume",
                        ctx.as_ref()
                            .map(|c| c.reason.as_str())
                            .unwrap_or("device error")
                    );
                    if let Some(ctx) = ctx {
                        self.requeue_after_rebuild(&ctx);
                    } else {
                        self.sink.pause();
                    }
                }
                Err(fail) => {
                    self.recovery.record_failure();
                    handle_error(&self.updater, "audio device recovery failed", Err(fail));
                }
            }
            return;
        }

        // Watchdog: a rebuild that never lands (device wedged in stream open)
        // must not keep recovery stuck forever. Count it as a failure so the
        // policy can eventually surface the DeviceLost screen.
        if let Some(started) = self.rebuild_started_at {
            if started.elapsed() >= REBUILD_TIMEOUT {
                warn!("Audio-device rebuild timed out; counting it as a failure");
                self.rebuild_in_flight = false;
                self.rebuild_started_at = None;
                self.pending_requeue = None;
                self.recovery.record_failure();
                handle_error(
                    &self.updater,
                    "audio device recovery failed",
                    Err(PlayError::StreamError(rodio::StreamError::NoDevice)),
                );
            }
        }
    }

    /// Re-queues the track that was current when the rebuild was requested, at
    /// its previous position, and pauses. Skipped when the user navigated away
    /// while the rebuild was in flight (the old track must not be resurrected).
    fn requeue_after_rebuild(&mut self, ctx: &RebuildContext) {
        match (ctx.downloaded, &ctx.video) {
            (true, Some(video)) => {
                let same_track = self
                    .current()
                    .map(|c| c.video_id == video.video_id)
                    .unwrap_or(false);
                if !same_track {
                    info!("Track changed during rebuild; skipping re-queue");
                } else {
                    let k = CACHE_DIR.join(format!("downloads/{}.mp4", video.video_id));
                    if let Err(e) = self.sink.resume_at(k.as_path(), ctx.position) {
                        // The device is fine; the cached file may be missing or
                        // corrupt. Let the normal update loop advance or clean it up.
                        error!("Could not requeue current track after rebuild: {:?}", e);
                    }
                }
            }
            _ => {
                info!("Rebuilt audio device; the current track will be queued when ready");
            }
        }
        // Pause on any recovery: never auto-resume. This guarantees the player
        // can't keep blasting a broken device, nor switch to another default
        // output (e.g. internal speakers after a headphone/dock removal). The
        // user explicitly resumes from the same spot.
        self.sink.pause();
    }
    fn update_controls(&mut self) {
        let current = self.current().cloned();
        let result = self
            .controls
            .update(current, &self.sink)
            .map_err(|x| format!("{x:?}"));
        handle_error::<String>(&self.updater, "Can't update finished media control", result);
    }
}

pub fn player_system(updater: Sender<ManagerMessage>) -> (Sender<SoundAction>, PlayerState) {
    let (tx, rx) = flume::unbounded::<SoundAction>();
    (tx.clone(), PlayerState::new(tx, rx, updater))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Async recovery: the rebuild must run off the UI thread ---
    //
    // Same ALSA-null technique as crates/player/tests/device_recovery.rs: the
    // default output device of this process is forced to the null PCM, so no
    // sound can ever reach the real speakers during the test.

    const TEST_ALSA_NULL_CONF: &str = r#"
# Silence: force the default output device to the null PCM.
pcm.!default {
    type null
}
"#;

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
            candidates.sort();
            if let Some(dir) = candidates.pop() {
                return dir;
            }
        }
        panic!("could not locate the stock ALSA config dir (alsa.conf)");
    }

    fn set_null_device() {
        let config_dir = find_alsa_config_dir();
        let stock = config_dir.join("alsa.conf");
        let override_path =
            std::env::temp_dir().join(format!("ytermusic-alsa-null-{}.conf", std::process::id()));
        std::fs::write(&override_path, TEST_ALSA_NULL_CONF).expect("write ALSA override config");
        let config_path = format!("{}:{}", stock.display(), override_path.display());
        std::env::set_var("ALSA_CONFIG_DIR", &config_dir);
        std::env::set_var("ALSA_CONFIG_PATH", &config_path);
    }

    #[test]
    fn recovery_runs_off_the_ui_thread_and_pauses() {
        set_null_device();

        let (updater, _updater_rx) = unbounded::<ManagerMessage>();
        let (_sound_tx, mut ps) = player_system(updater);

        // request_recovery must return immediately: it only spawns a worker
        // thread; the stream open happens there, never on the UI thread.
        let start = Instant::now();
        ps.request_recovery("test");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "request_recovery must not block the UI thread on the device"
        );

        // update() must keep returning promptly while the rebuild is in
        // flight, and eventually apply the result (swap + pause).
        while ps.rebuild_in_flight {
            let tick = Instant::now();
            ps.update();
            assert!(
                tick.elapsed() < Duration::from_secs(1),
                "update() must not block on the device rebuild"
            );
            assert!(
                start.elapsed() < REBUILD_TIMEOUT + Duration::from_secs(2),
                "recovery never landed"
            );
        }

        assert!(
            ps.sink.is_paused(),
            "pause-on-recovery must be preserved (never auto-resume)"
        );
    }
}
