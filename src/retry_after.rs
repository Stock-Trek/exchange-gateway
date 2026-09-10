use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) fn retry_after(headers: &[(String, String)]) -> Option<Duration> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("Retry-After"))
        .and_then(|(_, value)| parse_retry_after(value))
}

pub(crate) fn parse_retry_after(value: &str) -> Option<Duration> {
    let value = value.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let retry_at = parse_http_date(value)?;
    Some(
        retry_at
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO),
    )
}

fn parse_http_date(value: &str) -> Option<SystemTime> {
    let parts = value.split_whitespace().collect::<Vec<_>>();
    let (year, month, day, time) = match parts.as_slice() {
        [weekday, day, month, year, time, "GMT"] if weekday.ends_with(',') => (
            parse_year(year)?,
            parse_month(month)?,
            parse_day(day)?,
            parse_time(time)?,
        ),
        [weekday, date, time, "GMT"] if weekday.ends_with(',') => {
            let mut date = date.split('-');
            let day = parse_day(date.next()?)?;
            let month = parse_month(date.next()?)?;
            let year = parse_short_year(date.next()?)?;
            if date.next().is_some() {
                return None;
            }
            (year, month, day, parse_time(time)?)
        }
        [weekday, month, day, time, year] if !weekday.ends_with(',') => (
            parse_year(year)?,
            parse_month(month)?,
            parse_day(day)?,
            parse_time(time)?,
        ),
        _ => return None,
    };
    let (hour, minute, second) = time;
    let seconds = days_from_civil(year, month, day)
        .checked_mul(86_400)?
        .checked_add(i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second))?;
    let seconds = u64::try_from(seconds).ok()?;
    UNIX_EPOCH.checked_add(Duration::from_secs(seconds))
}

fn parse_time(value: &str) -> Option<(u32, u32, u32)> {
    let mut parts = value.split(':');
    let hour = parts.next()?.parse::<u32>().ok()?;
    let minute = parts.next()?.parse::<u32>().ok()?;
    let second = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    Some((hour, minute, second))
}

fn parse_day(value: &str) -> Option<u32> {
    let day = value.parse::<u32>().ok()?;
    (1..=31).contains(&day).then_some(day)
}

fn parse_month(value: &str) -> Option<u32> {
    Some(match value.to_ascii_lowercase().as_str() {
        "jan" => 1,
        "feb" => 2,
        "mar" => 3,
        "apr" => 4,
        "may" => 5,
        "jun" => 6,
        "jul" => 7,
        "aug" => 8,
        "sep" => 9,
        "oct" => 10,
        "nov" => 11,
        "dec" => 12,
        _ => return None,
    })
}

fn parse_year(value: &str) -> Option<i64> {
    let year = value.parse::<i64>().ok()?;
    (year >= 1970).then_some(year)
}

fn parse_short_year(value: &str) -> Option<i64> {
    match value.parse::<i64>().ok()? {
        year @ 0..=69 => Some(2000 + year),
        year @ 70..=99 => Some(1900 + year),
        _ => None,
    }
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day_of_year =
        (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}
