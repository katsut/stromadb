//! Deterministic calendar facets for timestamp rendering — the engine precomputes the derivations
//! LLM readers get wrong (weekday, day-delta to the query's as-of, business-hours), per the v2
//! finding that dated context lifts reader accuracy. Instants are epoch **seconds** UTC (the fact
//! model's `valid_from`); a `Calendar` frame adds timezone offset + business hours so a bare local
//! time is never rendered without its frame. No `chrono` dependency (civil-from-days is Hinnant's
//! well-known algorithm); DST is out of scope (fixed offset — documented, refined later).

/// A rendering frame: fixed UTC offset (minutes) + business-day window. The org/site calendar.
#[derive(Clone, Copy, Debug)]
pub struct Calendar {
    pub utc_offset_min: i32,          // e.g. +540 for JST, -240 for EDT
    pub business_start_min: u32,      // minutes from local midnight, e.g. 540 = 09:00
    pub business_end_min: u32,        // e.g. 1080 = 18:00
    pub fiscal_year_start_month: u32, // 1..=12; 4 = April-start fiscal year
}

impl Default for Calendar {
    /// UTC, 09:00–18:00, calendar-year fiscal.
    fn default() -> Self {
        Calendar {
            utc_offset_min: 0,
            business_start_min: 540,
            business_end_min: 1080,
            fiscal_year_start_month: 1,
        }
    }
}

const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

/// Civil date (year, month 1..=12, day 1..=31) from days since the Unix epoch. Hinnant's algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// A timestamp's calendar facets under a frame — the precomputed derivations handed to the reader.
#[derive(Clone, Debug, PartialEq)]
pub struct Stamp {
    pub iso: String,           // "2023-05-30 08:40 +09:00" (local, framed)
    pub weekday: &'static str, // local weekday
    pub in_business_hours: bool,
    pub fiscal_year: i64,    // fiscal year the date falls in (start-month aware)
    pub fiscal_quarter: u32, // 1..=4
    pub rel_days_to_asof: i64, // asof_day - this_day; +N = N days before the question, -N = after, 0 = same day
}

impl Calendar {
    /// Render epoch-seconds `t` under this frame, relative to `as_of` (also epoch seconds).
    pub fn stamp(&self, t: i64, as_of: i64) -> Stamp {
        let local = t + self.utc_offset_min as i64 * 60;
        let days = local.div_euclid(86_400);
        let secs = local.rem_euclid(86_400);
        let (y, m, d) = civil_from_days(days);
        let (hh, mm) = ((secs / 3600) as u32, ((secs % 3600) / 60) as u32);
        let weekday_idx = (days.rem_euclid(7) + 4).rem_euclid(7); // 0 = Sunday
        let weekday = WEEKDAYS[weekday_idx as usize];
        let min_of_day = (secs / 60) as u32;
        let in_business_hours = (1..=5).contains(&weekday_idx) // Mon..Fri
            && min_of_day >= self.business_start_min
            && min_of_day < self.business_end_min;
        // fiscal year/quarter (start-month aware)
        let fy_start = self.fiscal_year_start_month as i64;
        let months_in = ((m as i64) - fy_start).rem_euclid(12);
        let fiscal_year = if (m as i64) >= fy_start { y } else { y - 1 };
        let fiscal_quarter = (months_in / 3 + 1) as u32;
        let off_h = self.utc_offset_min / 60;
        let off_m = (self.utc_offset_min % 60).abs();
        let sign = if self.utc_offset_min < 0 { '-' } else { '+' };
        let iso = format!(
            "{y:04}-{m:02}-{d:02} {hh:02}:{mm:02} {sign}{:02}:{off_m:02}",
            off_h.abs()
        );
        let asof_local = as_of + self.utc_offset_min as i64 * 60;
        let rel_days_to_asof = asof_local.div_euclid(86_400) - days;
        Stamp {
            iso,
            weekday,
            in_business_hours,
            fiscal_year,
            fiscal_quarter,
            rel_days_to_asof,
        }
    }

    /// One-line human tag for a context excerpt, e.g.
    /// "2023-05-30 08:40 +09:00 (Tue, 26 days before, business hours, FY2023 Q1)".
    pub fn tag(&self, t: i64, as_of: i64) -> String {
        let s = self.stamp(t, as_of);
        let rel = match s.rel_days_to_asof {
            0 => "same day".to_string(),
            n if n > 0 => format!("{n} days before"),
            n => format!("{} days after", -n),
        };
        let bh = if s.in_business_hours {
            ", business hours"
        } else {
            ""
        };
        format!(
            "{} ({}, {}{}, FY{} Q{})",
            s.iso, s.weekday, rel, bh, s.fiscal_year, s.fiscal_quarter
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 2023-05-30 08:40 JST == 2023-05-29 23:40 UTC == epoch 1685403600
    const T: i64 = 1_685_403_600;

    #[test]
    fn utc_date_weekday() {
        let c = Calendar::default(); // UTC
        let s = c.stamp(T, T);
        assert!(s.iso.starts_with("2023-05-29 23:40 +00:00"), "{}", s.iso);
        assert_eq!(s.weekday, "Mon"); // 2023-05-29 is a Monday
        assert_eq!(s.rel_days_to_asof, 0);
    }

    #[test]
    fn jst_frame_shifts_local_civil_day() {
        let c = Calendar {
            utc_offset_min: 540,
            ..Calendar::default()
        };
        let s = c.stamp(T, T);
        assert!(s.iso.starts_with("2023-05-30 08:40 +09:00"), "{}", s.iso);
        assert_eq!(s.weekday, "Tue"); // crosses midnight into Tuesday in Tokyo
        assert!(!s.in_business_hours); // 08:40 is before the default 09:00 start
    }

    #[test]
    fn business_hours_flag() {
        let c = Calendar {
            utc_offset_min: 540,
            business_start_min: 480,
            ..Calendar::default()
        }; // 08:00 start
        assert!(c.stamp(T, T).in_business_hours); // Tue 08:40, window 08:00-18:00
    }

    #[test]
    fn relative_days() {
        let c = Calendar::default();
        let asof = T + 26 * 86_400;
        assert_eq!(c.stamp(T, asof).rel_days_to_asof, 26); // T is 26 days before as_of
    }

    #[test]
    fn fiscal_year_april_start() {
        let c = Calendar {
            fiscal_year_start_month: 4,
            ..Calendar::default()
        };
        let s = c.stamp(T, T); // 2023-05-29 -> FY2023 (Apr-start), Q1 (Apr-Jun)
        assert_eq!(s.fiscal_year, 2023);
        assert_eq!(s.fiscal_quarter, 1);
        // a January date falls in the prior fiscal year, Q4
        let jan = 1_672_617_600; // 2023-01-02 00:00 UTC
        let sj = c.stamp(jan, jan);
        assert_eq!(sj.fiscal_year, 2022);
        assert_eq!(sj.fiscal_quarter, 4);
    }
}
