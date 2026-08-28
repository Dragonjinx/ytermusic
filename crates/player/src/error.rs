// Custom Error Enum to handle different failures
#[derive(Debug)]
pub enum PlayError {
    Io(std::io::Error),
    DecoderError(rodio::decoder::DecoderError),
    StreamError(rodio::StreamError),
    PlayError(rodio::PlayError),
    SeekError(rodio::source::SeekError),
    /// A runtime failure of the audio output stream reported by the device itself
    /// (e.g. the device became unavailable, or its stream got corrupted after a
    /// system suspend/resume, which on some hardware results in a dead channel
    /// or a constant beep). Recovering from this requires rebuilding the output
    /// stream so the device re-negotiates with the driver.
    DeviceStreamError(rodio::cpal::StreamError),
}

impl PlayError {
    /// Returns true when the failure originates from the audio device itself
    /// (the device disappeared or its stream became unusable), as opposed to
    /// a file/decoder/seek problem. Such errors require rebuilding the output
    /// stream to recover.
    pub fn is_device_loss(&self) -> bool {
        matches!(self, Self::DeviceStreamError(_) | Self::StreamError(_))
    }
}

impl From<rodio::PlayError> for PlayError {
    fn from(err: rodio::PlayError) -> Self {
        PlayError::PlayError(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend_err(description: &str) -> rodio::cpal::StreamError {
        rodio::cpal::StreamError::BackendSpecific {
            err: rodio::cpal::BackendSpecificError {
                description: description.to_owned(),
            },
        }
    }

    fn io_err() -> PlayError {
        PlayError::Io(std::io::Error::new(std::io::ErrorKind::Other, "test"))
    }

    #[test]
    fn device_stream_errors_are_device_loss() {
        assert!(PlayError::DeviceStreamError(backend_err("x")).is_device_loss());
        assert!(
            PlayError::DeviceStreamError(rodio::cpal::StreamError::DeviceNotAvailable)
                .is_device_loss()
        );
    }

    #[test]
    fn stream_open_errors_are_device_loss() {
        assert!(PlayError::StreamError(rodio::StreamError::NoDevice).is_device_loss());
    }

    #[test]
    fn file_and_seek_errors_are_not_device_loss() {
        assert!(!io_err().is_device_loss());
        assert!(
            !PlayError::SeekError(rodio::source::SeekError::NotSupported {
                underlying_source: "test"
            })
            .is_device_loss()
        );
    }
}
