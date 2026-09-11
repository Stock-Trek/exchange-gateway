use std::time::{Duration, SystemTime};

pub(crate) struct RetryAfter;

impl RetryAfter {
    pub(crate) fn from_headers(headers: &[(String, String)]) -> Option<Duration> {
        headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("Retry-After"))
            .and_then(|(_, value)| Self::parse_retry_after(value))
    }
    fn parse_retry_after(value: &str) -> Option<Duration> {
        let value = value.trim();
        if let Ok(seconds) = value.parse::<u64>() {
            return Some(Duration::from_secs(seconds));
        }
        let retry_at = httpdate::parse_http_date(value).ok()?;
        Some(
            retry_at
                .duration_since(SystemTime::now())
                .unwrap_or(Duration::ZERO),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_delta_seconds() {
        assert_eq!(
            RetryAfter::parse_retry_after("120"),
            Some(Duration::from_secs(120))
        );
    }

    #[test]
    fn ignores_invalid_values() {
        assert_eq!(RetryAfter::parse_retry_after("not-a-date"), None);
    }

    #[test]
    fn parses_http_date_forms() {
        for value in [
            "Sun, 06 Nov 1994 08:49:37 GMT",
            "Sunday, 06-Nov-94 08:49:37 GMT",
            "Sun Nov  6 08:49:37 1994",
        ] {
            assert_eq!(RetryAfter::parse_retry_after(value), Some(Duration::ZERO));
        }
    }

    #[test]
    fn parses_http_date_in_the_future() {
        let value = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(3_600));
        let parsed = RetryAfter::parse_retry_after(&value).expect("future HTTP-date should parse");
        assert!(parsed > Duration::from_secs(3_500) && parsed <= Duration::from_secs(3_600));
    }
}
