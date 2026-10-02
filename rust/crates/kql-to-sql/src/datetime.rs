//! Kusto datetime literal parsing and formatting (UTC, microsecond resolution).

const US_PER_SEC: i64 = 1_000_000;
const US_PER_DAY: i64 = 86_400 * US_PER_SEC;

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = m as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        _ => 28,
    }
}

/// Builds microseconds since the epoch, validating ranges.
pub fn make_us(y: i64, mo: u32, d: u32, h: u32, mi: u32, s: u32, frac_us: i64) -> Option<i64> {
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    Some(days_from_civil(y, mo, d) * US_PER_DAY + (h as i64 * 3600 + mi as i64 * 60 + s as i64) * US_PER_SEC + frac_us)
}

const MONTHS: [&str; 12] = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];

/// Parses the text of a Kusto `datetime(...)` literal into microseconds since the epoch.
pub fn parse_datetime(text: &str) -> Option<i64> {
    let t = text.trim();
    if t.is_empty() {
        return None;
    }
    parse_iso(t).or_else(|| parse_rfc(t)).or_else(|| parse_us_style(t))
}

struct Cursor<'a> {
    s: &'a [u8],
    i: usize,
}

impl Cursor<'_> {
    fn num(&mut self, min: usize, max: usize) -> Option<i64> {
        let start = self.i;
        while self.i < self.s.len() && self.i - start < max && self.s[self.i].is_ascii_digit() {
            self.i += 1;
        }
        if self.i - start < min {
            return None;
        }
        std::str::from_utf8(&self.s[start..self.i]).ok()?.parse().ok()
    }
    fn eat(&mut self, c: u8) -> bool {
        if self.s.get(self.i) == Some(&c) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }
    fn done(&self) -> bool {
        self.i >= self.s.len()
    }
}

/// `yyyy-MM-dd[( |T)HH:mm[:ss[.fffffff]]][Z|±hh:mm]`, also `yyyyMMdd`.
fn parse_iso(t: &str) -> Option<i64> {
    let mut c = Cursor { s: t.as_bytes(), i: 0 };
    let y = c.num(4, 4)?;
    let (mo, d);
    if c.eat(b'-') {
        mo = c.num(1, 2)? as u32;
        if !c.eat(b'-') {
            return None;
        }
        d = c.num(1, 2)? as u32;
    } else if c.eat(b'/') {
        mo = c.num(1, 2)? as u32;
        if !c.eat(b'/') {
            return None;
        }
        d = c.num(1, 2)? as u32;
    } else {
        mo = c.num(2, 2)? as u32;
        d = c.num(2, 2)? as u32;
    }
    let (mut h, mut mi, mut s, mut frac) = (0, 0, 0, 0i64);
    if c.eat(b'T') || c.eat(b't') || c.eat(b' ') {
        while c.eat(b' ') {}
        if !c.done() {
            h = c.num(1, 2)? as u32;
            if c.eat(b':') {
                mi = c.num(1, 2)? as u32;
                if c.eat(b':') {
                    s = c.num(1, 2)? as u32;
                    if c.eat(b'.') {
                        let start = c.i;
                        while c.peek().is_some_and(|b| b.is_ascii_digit()) {
                            c.i += 1;
                        }
                        let digits = &t[start..c.i];
                        let mut us = String::from(digits);
                        us.truncate(6);
                        while us.len() < 6 {
                            us.push('0');
                        }
                        frac = us.parse().ok()?;
                    }
                }
            }
        }
    }
    let mut offset_us = 0;
    while c.eat(b' ') {}
    if c.eat(b'Z') || c.eat(b'z') {
    } else if matches!(c.peek(), Some(b'+' | b'-')) {
        let sign = if c.eat(b'-') { -1 } else {
            c.eat(b'+');
            1
        };
        let oh = c.num(2, 2)?;
        c.eat(b':');
        let om = c.num(0, 2).unwrap_or(0);
        offset_us = sign * (oh * 3600 + om * 60) * US_PER_SEC;
    } else if t[c.i..].eq_ignore_ascii_case("gmt") || t[c.i..].eq_ignore_ascii_case("utc") {
        c.i = t.len();
    }
    if !c.done() {
        return None;
    }
    Some(make_us(y, mo, d, h, mi, s, frac)? - offset_us)
}

/// RFC-822/1123 style: `Sat, 8 Nov 2014 15:05:02 GMT`, `8 Nov 2014 15:05:02`.
fn parse_rfc(t: &str) -> Option<i64> {
    let t = match t.find(',') {
        Some(i) => t[i + 1..].trim(),
        None => t,
    };
    let parts: Vec<&str> = t.split_whitespace().collect();
    if parts.len() < 3 {
        return None;
    }
    let d: u32 = parts[0].parse().ok()?;
    let mo = MONTHS.iter().position(|m| parts[1].to_ascii_lowercase().starts_with(m))? as u32 + 1;
    let mut y: i64 = parts[2].parse().ok()?;
    if y < 100 {
        y += 2000;
    }
    let (mut h, mut mi, mut s) = (0, 0, 0);
    if let Some(time) = parts.get(3) {
        let hms: Vec<&str> = time.split(':').collect();
        h = hms.first()?.parse().ok()?;
        mi = hms.get(1).map(|x| x.parse().ok()).unwrap_or(Some(0))?;
        s = hms.get(2).map(|x| x.parse().ok()).unwrap_or(Some(0))?;
    }
    make_us(y, mo, d, h, mi, s, 0)
}

/// `MM/dd/yyyy [HH:mm[:ss]]`
fn parse_us_style(t: &str) -> Option<i64> {
    let (date, time) = match t.split_once(' ') {
        Some((a, b)) => (a, Some(b.trim())),
        None => (t, None),
    };
    let p: Vec<&str> = date.split('/').collect();
    if p.len() != 3 || p[2].len() != 4 {
        return None;
    }
    let mo: u32 = p[0].parse().ok()?;
    let d: u32 = p[1].parse().ok()?;
    let y: i64 = p[2].parse().ok()?;
    let (mut h, mut mi, mut s) = (0, 0, 0);
    if let Some(time) = time {
        let hms: Vec<&str> = time.split(':').collect();
        h = hms.first()?.parse().ok()?;
        mi = hms.get(1).map(|x| x.parse().ok()).unwrap_or(Some(0))?;
        s = hms.get(2).map(|x| x.parse().ok()).unwrap_or(Some(0))?;
    }
    make_us(y, mo, d, h, mi, s, 0)
}

/// `yyyy-MM-dd HH:mm:ss[.ffffff]` for SQL TIMESTAMP literals.
pub fn format_sql(us: i64) -> String {
    let days = us.div_euclid(US_PER_DAY);
    let rem = us.rem_euclid(US_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    let secs = rem / US_PER_SEC;
    let frac = rem % US_PER_SEC;
    let base = format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60);
    if frac == 0 {
        base
    } else {
        format!("{base}.{frac:06}")
    }
}

/// ISO 8601 with 7 fractional digits and `Z`, as Kusto prints datetimes.
pub fn format_iso(us: i64) -> String {
    let days = us.div_euclid(US_PER_DAY);
    let rem = us.rem_euclid(US_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    let secs = rem / US_PER_SEC;
    let frac = rem % US_PER_SEC;
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{frac:06}0Z", secs / 3600, secs / 60 % 60, secs % 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses() {
        assert_eq!(parse_datetime("1970-01-01"), Some(0));
        assert_eq!(parse_datetime("2020-01-01 10:00").map(format_sql).as_deref(), Some("2020-01-01 10:00:00"));
        assert_eq!(parse_datetime("2015-12-31 23:59:59.9").map(format_sql).as_deref(), Some("2015-12-31 23:59:59.900000"));
        assert_eq!(parse_datetime("2014-05-25T08:20:03.123456Z").map(format_sql).as_deref(), Some("2014-05-25 08:20:03.123456"));
        assert_eq!(parse_datetime("2024-02-29 23:59:59.9999999").map(format_sql).as_deref(), Some("2024-02-29 23:59:59.999999"));
        assert_eq!(parse_datetime("Sat, 8 Nov 2014 15:05:02 GMT").map(format_sql).as_deref(), Some("2014-11-08 15:05:02"));
        assert_eq!(parse_datetime("1960-03-01").map(format_sql).as_deref(), Some("1960-03-01 00:00:00"));
        assert_eq!(parse_datetime("2020-02-30"), None);
        assert_eq!(parse_datetime("2020-01-01T00:00:00+02:00").map(format_sql).as_deref(), Some("2019-12-31 22:00:00"));
    }
}
