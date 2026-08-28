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

/// How long the output-position must stay frozen (while the player thinks it
/// is playing) before we treat the audio device as stalled. Borrowed from the
/// stall-detection used by other Linux players (e.g. the `decal` audio
/// backend discussed in termusic#428): a healthy stream always advances its
/// position, so a frozen position while not paused/empty means audio can no
/// longer reach the device.
const STALL_THRESHOLD: Duration = Duration::from_millis(1000);

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
    /// Last observed sink position, used for stall detection.
    last_position: Duration,
    /// When the sink position was last observed advancing.
    last_position_advance: Instant,
    last_download_list: Vec<String>,
}

impl PlayerState {
    fn new(
        soundaction_sender: Sender<SoundAction>,
        soundaction_receiver: Receiver<SoundAction>,
        updater: Sender<ManagerMessage>,
    ) -> Self {
        let (stream_error_sender, stream_error_receiver) = unbounded::<PlayError>();
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
            last_position: Duration::ZERO,
            last_position_advance: Instant::now(),
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
        PLAYER_RUNNING.store(self.current().is_some(), Ordering::SeqCst);
        self.update_controls();
        self.handle_stream_errors();
        self.detect_stall();
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

    /// Detects when the audio device silently died even though cpal never
    /// fired its error callback (e.g. a device removed in a way that leaves
    /// the ALSA/stream state frozen, or a stream that PipeWire stopped
    /// draining). A healthy playing stream always advances its position, so a
    /// frozen position while not paused/empty means no audio is reaching the
    /// device -> recover (rebuild + pause).
    fn detect_stall(&mut self) {
        // When paused, between tracks, or at a natural end, the position is
        // legitimately frozen: just reset the baseline and wait.
        if self.sink.is_paused() || self.sink.is_finished() {
            self.last_position = self.sink.elapsed();
            self.last_position_advance = Instant::now();
            return;
        }

        let now = Instant::now();
        let position = self.sink.elapsed();
        if is_stalled_at(
            position,
            self.last_position,
            now.duration_since(self.last_position_advance),
        ) {
            warn!("Audio output stalled (position frozen while playing); pausing + rebuilding");
            self.request_recovery("output stalled");
            // Reset the watchdog so we don't re-fire on every tick if the
            // rebuild is throttled by the recovery cooldown.
            self.last_position = self.sink.elapsed();
            self.last_position_advance = Instant::now();
        } else {
            self.last_position = position;
            self.last_position_advance = now;
        }
    }

    /// Rebuilds the audio output stream and pauses playback. This is the
    /// single entry point for any device problem (cpal stream error or a
    /// detected stall). Rate-limited by the [`RecoveryPolicy`]; gives up after
    /// repeated failures by surfacing a DeviceLost screen.
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
        if !self.recovery.should_attempt(now) {
            // A rebuild is already settling; drop this (logged above).
            return;
        }
        self.recovery.record_attempt(now);
        match self.rebuild_device() {
            Ok(()) => {
                info!("Recovered audio device ({reason}) and paused; press play to resume");
            }
            Err(fail) => {
                handle_error(&self.updater, "audio device recovery failed", Err(fail));
            }
        }
    }

    /// Rebuilds the audio output stream and re-queues the current track at its
    /// previous position, leaving it **paused**.
    ///
    /// Returns Err when the output device could not be reopened (the caller
    /// surfaces a DeviceLost screen in that case). Records the outcome in the
    /// [`RecoveryPolicy`] (success resets the consecutive-failure counter,
    /// failure increments it), so both the automatic path and the manual
    /// DeviceLost retry (`RestartPlayer`) keep the policy in sync.
    pub(crate) fn rebuild_device(&mut self) -> Result<(), PlayError> {
        let position = self.sink.elapsed();
        let video = self.current().cloned();
        let downloaded = self.is_current_downloaded();

        let updated = match self.sink.update() {
            Ok(updated) => updated,
            Err(e) => {
                self.recovery.record_failure();
                return Err(e);
            }
        };
        self.sink = updated;
        // The device rebuilt successfully: whatever happens to the replay
        // (e.g. the cached file is missing) is not a device problem, so the
        // failure counter is reset.
        self.recovery.record_success();

        if let (true, Some(video)) = (downloaded, video) {
            let k = CACHE_DIR.join(format!("downloads/{}.mp4", video.video_id));
            if let Err(e) = self.sink.resume_at(k.as_path(), position) {
                // The device is fine; the cached file may be missing or
                // corrupt. Let the normal update loop advance or clean it up.
                error!("Could not requeue current track after rebuild: {:?}", e);
            }
        } else {
            info!("Rebuilt audio device; the current track will be queued when ready");
        }

        // Pause on any recovery: never auto-resume. This guarantees the player
        // can't keep blasting a broken device, nor switch to another default
        // output (e.g. internal speakers after a headphone/dock removal). The
        // user explicitly resumes from the same spot.
        self.sink.pause();
        Ok(())
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

/// Pure stall-detection predicate: a healthy playing stream always advances
/// its position, so a position frozen for >= `STALL_THRESHOLD` (while not
/// paused/empty, handled by the caller) means audio is no longer reaching the
/// device.
fn is_stalled_at(position: Duration, last_position: Duration, since_advance: Duration) -> bool {
    position == last_position && since_advance >= STALL_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: Duration = STALL_THRESHOLD;

    #[test]
    fn advancing_position_is_not_a_stall() {
        assert!(!is_stalled_at(
            Duration::from_secs(5),
            Duration::from_secs(4),
            T
        ));
    }

    #[test]
    fn frozen_below_threshold_is_not_yet_a_stall() {
        assert!(!is_stalled_at(
            Duration::from_secs(5),
            Duration::from_secs(5),
            T - Duration::from_millis(1)
        ));
    }

    #[test]
    fn frozen_at_threshold_is_a_stall() {
        assert!(is_stalled_at(
            Duration::from_secs(5),
            Duration::from_secs(5),
            T
        ));
    }

    #[test]
    fn frozen_past_threshold_is_a_stall() {
        assert!(is_stalled_at(
            Duration::from_secs(5),
            Duration::from_secs(5),
            T + Duration::from_secs(2)
        ));
    }
}
