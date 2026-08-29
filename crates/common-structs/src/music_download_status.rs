/// Spinner frames for the pre-download (setup) phase. Braille block (U+2800),
/// the same canonical sequence pi uses — standard Unicode, no nerdfont needed.
pub const SPINNER_FRAMES: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum MusicDownloadStatus {
    NotDownloaded,
    Downloaded,
    /// Actively downloading with a known percentage.
    Downloading(usize),
    /// yt-dlp is still resolving/negotiating; no bytes counted yet. The `usize`
    /// is a frame index into [`SPINNER_FRAMES`] so the TUI can animate it.
    Spinner(usize),
    DownloadFailed,
}

impl MusicDownloadStatus {
    pub fn character(&self, playing: Option<bool>) -> String {
        let s = match self {
            Self::NotDownloaded => {
                if let Some(e) = playing {
                    if e { '▶' } else { '⏸' }
                } else {
                    ' '
                }
            }
            Self::Downloaded => ' ',
            Self::Downloading(progress) => {
                // Always exactly 3 chars between the brackets so the column
                // never shifts: `[01%]`..`[99%]`, and a centered check at 100%
                // (a 3-digit + `%` would be 4 wide).
                if *progress >= 100 {
                    return format!("⭳ [ ✓ ]");
                }
                return format!("⭳ [{:02}%]", progress);
            }
            Self::Spinner(frame) => {
                return format!("⭳ [ {} ]", SPINNER_FRAMES[frame % SPINNER_FRAMES.len()]);
            }
            Self::DownloadFailed => '⚠',
        };
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The download status block must always be exactly 7 chars wide
    /// ("⭳ [xxx]") — spinner, single- and double-digit percents and the
    /// 100% check — so the playlist column never shifts while downloading.
    #[test]
    fn downloading_and_spinner_stay_fixed_width() {
        assert_eq!(
            MusicDownloadStatus::Spinner(0)
                .character(None)
                .chars()
                .count(),
            7
        );
        assert_eq!(
            MusicDownloadStatus::Spinner(5)
                .character(None)
                .chars()
                .count(),
            7
        );
        for p in [0, 1, 12, 50, 99] {
            assert_eq!(
                MusicDownloadStatus::Downloading(p)
                    .character(None)
                    .chars()
                    .count(),
                7,
                "percent {p} should stay 3-wide inside the brackets"
            );
        }
        assert_eq!(
            MusicDownloadStatus::Downloading(100)
                .character(None)
                .chars()
                .count(),
            7
        );
    }

    #[test]
    fn hundred_percent_shows_check() {
        assert_eq!(
            MusicDownloadStatus::Downloading(100).character(None),
            "⭳ [ ✓ ]"
        );
        assert_eq!(
            MusicDownloadStatus::Downloading(99).character(None),
            "⭳ [99%]"
        );
        assert_eq!(
            MusicDownloadStatus::Downloading(1).character(None),
            "⭳ [01%]"
        );
        assert_eq!(MusicDownloadStatus::Spinner(3).character(None), "⭳ [ ⠸ ]");
    }
}
