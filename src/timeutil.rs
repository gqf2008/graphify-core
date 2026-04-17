use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UtcDateTime {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
}

impl UtcDateTime {
    pub fn date_string(&self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }

    pub fn iso_string(&self) -> String {
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}+00:00",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )
    }

    pub fn filename_stamp(&self) -> String {
        format!(
            "{:04}{:02}{:02}_{:02}{:02}{:02}",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )
    }
}

pub fn current_utc_datetime() -> UtcDateTime {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    datetime_from_unix(now)
}

fn datetime_from_unix(seconds: i64) -> UtcDateTime {
    let days = seconds.div_euclid(86_400);
    let secs_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);

    UtcDateTime {
        year,
        month,
        day,
        hour: (secs_of_day / 3_600) as u32,
        minute: ((secs_of_day % 3_600) / 60) as u32,
        second: (secs_of_day % 60) as u32,
    }
}

fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = (yoe + era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = (mp + if mp < 10 { 3 } else { -9 }) as u32;
    if month <= 2 {
        year += 1;
    }
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::datetime_from_unix;

    #[test]
    fn datetime_from_unix_formats_epoch() {
        let dt = datetime_from_unix(0);
        assert_eq!(dt.date_string(), "1970-01-01");
        assert_eq!(dt.iso_string(), "1970-01-01T00:00:00+00:00");
        assert_eq!(dt.filename_stamp(), "19700101_000000");
    }

    #[test]
    fn datetime_from_unix_handles_recent_date() {
        let dt = datetime_from_unix(1_712_752_496);
        assert_eq!(dt.date_string(), "2024-04-10");
        assert_eq!(dt.iso_string(), "2024-04-10T12:34:56+00:00");
    }
}
