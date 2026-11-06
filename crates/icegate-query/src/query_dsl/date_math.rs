//! Dates of range bounds and date terms, and the date math they may carry.
//!
//! A bound resolves at millisecond precision, the precision IceGate gives `date` values, and is
//! returned in microseconds, the unit of IceGate's timestamp columns, already turned into a
//! half-open comparison: a lower side keeps values `>=` the result, an upper side keeps values
//! `<` it. The side decides two things:
//!
//! - rounding: `gt` and `lte` round up, `gte` and `lt` round down. Rounding applies to a
//!   `/unit` in date math and to the fields a date omits (`lte 2026-10-08` covers the whole day);
//!   rounding up lands on the last millisecond of the unit;
//! - the millisecond meaning: a lower inclusive `X ms` keeps `>= X·1000 µs`, a lower exclusive
//!   one `>= (X+1)·1000`, an upper inclusive one `< (X+1)·1000`, an upper exclusive one
//!   `< X·1000`.
//!
//! `now` is a parameter, never read from the clock: one request evaluates one instant, and
//! tests state literal instants.

use chrono::{
    DateTime, Datelike, FixedOffset, LocalResult, Months, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeDelta,
    TimeZone, Timelike, Utc,
};
use chrono_tz::Tz;

use crate::opensearch::error::OpenSearchError;

/// The zone of a bound without `time_zone`.
const DEFAULT_ZONE: TimeZoneSpec = TimeZoneSpec::Named(Tz::UTC);

/// The largest magnitude of a fixed offset, in hours.
const MAX_OFFSET_HOURS: i32 = 18;

/// The date formats a range accepts by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DateFormat {
    /// `strict_date_optional_time`: `yyyy[-MM[-dd[THH[:mm[:ss[.fraction]]][offset]]]]`.
    StrictDateOptionalTime,
    /// `epoch_millis`: integer milliseconds since the epoch.
    EpochMillis,
    /// `date_time`: a full date and time with a fraction and an offset.
    DateTime,
}

impl DateFormat {
    /// Parse a `format` name.
    ///
    /// # Errors
    ///
    /// `parsing_exception` for any other format, naming it.
    pub(crate) fn parse(name: &str) -> Result<Self, OpenSearchError> {
        match name {
            "strict_date_optional_time" => Ok(Self::StrictDateOptionalTime),
            "epoch_millis" => Ok(Self::EpochMillis),
            "date_time" => Ok(Self::DateTime),
            other => Err(OpenSearchError::parsing(format!("unsupported date format [{other}]"))),
        }
    }
}

/// A `time_zone`: a fixed offset or an IANA zone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TimeZoneSpec {
    /// A fixed offset such as `+05:30`.
    Fixed(FixedOffset),
    /// An IANA zone such as `Europe/Berlin`, with its daylight-saving rules.
    Named(Tz),
}

impl TimeZoneSpec {
    /// The wall-clock time this zone shows at `instant`; `None` when the offset pushes that time
    /// outside the range `chrono` represents, which a representable `instant` can still do.
    fn to_local(&self, instant: DateTime<Utc>) -> Option<NaiveDateTime> {
        let offset = match self {
            Self::Fixed(offset) => *offset,
            Self::Named(zone) => zone.offset_from_utc_datetime(&instant.naive_utc()).fix(),
        };
        instant.naive_utc().checked_add_offset(offset)
    }

    /// The instant of `local_time` in this zone, as [`resolve_local_time`] places it.
    fn to_instant(&self, local_time: NaiveDateTime) -> Result<DateTime<Utc>, OpenSearchError> {
        match self {
            Self::Fixed(offset) => resolve_local_time(offset, local_time),
            Self::Named(zone) => resolve_local_time(zone, local_time),
        }
    }
}

/// The instant of `local_time` in `zone`. An ambiguous local time (the repeated hour) takes the
/// earlier instant; a local time inside a gap moves forward by the gap, using the offset in
/// force before it.
///
/// # Errors
///
/// `illegal_argument_exception` when the instant lies outside the range `chrono` represents.
fn resolve_local_time<Z: TimeZone>(zone: &Z, local_time: NaiveDateTime) -> Result<DateTime<Utc>, OpenSearchError> {
    match zone.from_local_datetime(&local_time) {
        LocalResult::Single(instant) | LocalResult::Ambiguous(instant, _) => Ok(instant.with_timezone(&Utc)),
        LocalResult::None => {
            let refuse_local_time =
                || OpenSearchError::illegal_argument(format!("no instant exists for the local time [{local_time}]"));
            let before_gap = local_time
                .checked_sub_signed(TimeDelta::days(1))
                .and_then(|earlier_time| zone.from_local_datetime(&earlier_time).earliest())
                .ok_or_else(refuse_local_time)?;
            let offset_seconds = before_gap.offset().fix().local_minus_utc();
            local_time
                .checked_sub_signed(TimeDelta::seconds(i64::from(offset_seconds)))
                .map(|instant| instant.and_utc())
                .ok_or_else(refuse_local_time)
        }
    }
}

/// Parse a `time_zone`: an IANA name (`Europe/Berlin`, `UTC`) or an offset `±HH`, `±HHMM`,
/// `±HH:MM` of at most [`MAX_OFFSET_HOURS`] hours.
///
/// # Errors
///
/// `parsing_exception` naming any other text.
pub(crate) fn parse_time_zone(text: &str) -> Result<TimeZoneSpec, OpenSearchError> {
    let refuse_time_zone = || OpenSearchError::parsing(format!("unknown time zone [{text}]"));
    if text.starts_with(['+', '-']) {
        return parse_offset(text).map(TimeZoneSpec::Fixed).ok_or_else(refuse_time_zone);
    }
    text.parse::<Tz>().map(TimeZoneSpec::Named).map_err(|_| refuse_time_zone())
}

/// `±HH`, `±HHMM` or `±HH:MM` of at most [`MAX_OFFSET_HOURS`] hours; `None` for anything else.
fn parse_offset(text: &str) -> Option<FixedOffset> {
    let sign = if text.starts_with('-') { -1 } else { 1 };
    let digits = text.strip_prefix(['+', '-'])?;
    let (hours, minutes) = match digits.len() {
        2 => (digits, "00"),
        4 => (digits.get(..2)?, digits.get(2..)?),
        5 if digits.as_bytes().get(2) == Some(&b':') => (digits.get(..2)?, digits.get(3..)?),
        _ => return None,
    };
    if !hours.bytes().chain(minutes.bytes()).all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let hours: i32 = hours.parse().ok()?;
    let minutes: i32 = minutes.parse().ok()?;
    let total_minutes = hours * 60 + minutes;
    if minutes > 59 || total_minutes > MAX_OFFSET_HOURS * 60 {
        return None;
    }
    FixedOffset::east_opt(sign * total_minutes * 60)
}

/// Which end of a range a date resolves for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoundSide {
    /// `gte`, and the start of a date term.
    LowerInclusive,
    /// `gt`.
    LowerExclusive,
    /// `lte`, and the end of a date term.
    UpperInclusive,
    /// `lt`.
    UpperExclusive,
}

/// Resolve a range bound to microseconds since the epoch.
///
/// `text` is a date (per `format`; without one, `strict_date_optional_time` then
/// `epoch_millis`), `now`, or `<date>||`, followed by date math: `+N<unit>`, `-N<unit>`,
/// `/<unit>` with units `y M w d h H m s`. `time_zone` places dates without an offset and
/// drives rounding. The result follows the side conventions of the module documentation: a
/// lower side keeps values `>=` it, an upper side values `<` it.
///
/// # Errors
///
/// `illegal_argument_exception` naming `text` when it is not a date in the format, its math is
/// malformed, or the instant overflows; naming the local time instead when `time_zone` places
/// that time outside the range `chrono` represents.
pub(crate) fn resolve_date_bound(
    text: &str,
    format: Option<DateFormat>,
    time_zone: Option<&TimeZoneSpec>,
    now: DateTime<Utc>,
    side: BoundSide,
) -> Result<i64, OpenSearchError> {
    let zone = time_zone.unwrap_or(&DEFAULT_ZONE);
    let is_rounded_up = matches!(side, BoundSide::LowerExclusive | BoundSide::UpperInclusive);
    let millis = if let Some(math) = text.strip_prefix("now") {
        apply_date_math(now.timestamp_millis(), math, is_rounded_up, zone, text)?
    } else if let Some((anchor, math)) = text.split_once("||") {
        let anchor_millis = parse_date_text(anchor, format, zone, false)?;
        apply_date_math(anchor_millis, math, is_rounded_up, zone, text)?
    } else {
        parse_date_text(text, format, zone, is_rounded_up)?
    };
    let bound_millis = match side {
        BoundSide::LowerInclusive | BoundSide::UpperExclusive => Some(millis),
        BoundSide::LowerExclusive | BoundSide::UpperInclusive => millis.checked_add(1),
    };
    bound_millis
        .and_then(|bound| bound.checked_mul(1000))
        .ok_or_else(|| refuse_date(text))
}

/// A date value stored in a document: `strict_date_optional_time` in UTC, or epoch
/// milliseconds; `None` when it is neither. Fraction digits below the millisecond are dropped.
pub(crate) fn parse_stored_date_millis(text: &str) -> Option<i64> {
    parse_date_text(text, None, &DEFAULT_ZONE, false).ok()
}

/// The `illegal_argument_exception` naming a bound that does not resolve to an instant.
fn refuse_date(text: &str) -> OpenSearchError {
    OpenSearchError::illegal_argument(format!("failed to parse date [{text}]"))
}

/// The calendar fields a date states, coarsest first; what follows the stated field is omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Precision {
    /// `yyyy`.
    Year,
    /// `yyyy-MM`.
    Month,
    /// `yyyy-MM-dd`.
    Day,
    /// `yyyy-MM-ddTHH`.
    Hour,
    /// `yyyy-MM-ddTHH:mm`.
    Minute,
    /// `yyyy-MM-ddTHH:mm:ss`.
    Second,
    /// `yyyy-MM-ddTHH:mm:ss.fraction`.
    Fraction,
}

/// A parsed `strict_date_optional_time` text.
struct ParsedDate {
    /// The stated fields, with the omitted ones at their smallest value.
    local_time: NaiveDateTime,
    /// The finest field the text states.
    precision: Precision,
    /// The offset the text states; `None` leaves the date to the bound's zone.
    offset: Option<FixedOffset>,
}

/// Parse a date without math to epoch milliseconds. When `is_rounded_up`, the omitted fields
/// take their largest value (the last millisecond the text covers).
///
/// # Errors
///
/// `illegal_argument_exception` naming `text` when it is not a date in `format`.
fn parse_date_text(
    text: &str,
    format: Option<DateFormat>,
    zone: &TimeZoneSpec,
    is_rounded_up: bool,
) -> Result<i64, OpenSearchError> {
    let parsed_date = match format {
        Some(DateFormat::EpochMillis) => return parse_epoch_millis(text).ok_or_else(|| refuse_date(text)),
        Some(DateFormat::DateTime) => {
            parse_date_optional_time(text).filter(|date| date.precision == Precision::Fraction && date.offset.is_some())
        }
        Some(DateFormat::StrictDateOptionalTime) => parse_date_optional_time(text),
        None => match parse_date_optional_time(text) {
            Some(date) => Some(date),
            None => return parse_epoch_millis(text).ok_or_else(|| refuse_date(text)),
        },
    }
    .ok_or_else(|| refuse_date(text))?;
    let to_instant = |local_time: NaiveDateTime| {
        parsed_date.offset.map_or_else(
            || zone.to_instant(local_time),
            |offset| resolve_local_time(&offset, local_time),
        )
    };
    if !is_rounded_up || parsed_date.precision == Precision::Fraction {
        return Ok(to_instant(parsed_date.local_time)?.timestamp_millis());
    }
    let next_local_time =
        advance_by_precision(parsed_date.local_time, parsed_date.precision).ok_or_else(|| refuse_date(text))?;
    Ok(to_instant(next_local_time)?.timestamp_millis() - 1)
}

/// The start of the next unit of `precision` after `local_time`; `None` on overflow.
const fn advance_by_precision(local_time: NaiveDateTime, precision: Precision) -> Option<NaiveDateTime> {
    match precision {
        Precision::Year => local_time.checked_add_months(Months::new(12)),
        Precision::Month => local_time.checked_add_months(Months::new(1)),
        Precision::Day => local_time.checked_add_signed(TimeDelta::days(1)),
        Precision::Hour => local_time.checked_add_signed(TimeDelta::hours(1)),
        Precision::Minute => local_time.checked_add_signed(TimeDelta::minutes(1)),
        Precision::Second => local_time.checked_add_signed(TimeDelta::seconds(1)),
        Precision::Fraction => Some(local_time),
    }
}

/// `-?[0-9]+` as milliseconds; `None` for any other text or one that overflows.
fn parse_epoch_millis(text: &str) -> Option<i64> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// The number ASCII `digits` spell. Callers read at most nine, which a `u32` always holds.
fn fold_ascii_digits(digits: &[u8]) -> u32 {
    digits.iter().fold(0, |value, digit| value * 10 + u32::from(digit - b'0'))
}

/// A byte cursor over an ASCII date.
struct Cursor<'a> {
    /// The date's bytes.
    bytes: &'a [u8],
    /// The offset of the next unread byte.
    position: usize,
}

impl Cursor<'_> {
    /// Step over the next byte when it is `expected`, and say whether it was.
    fn consume_byte(&mut self, expected: u8) -> bool {
        let is_next = self.bytes.get(self.position) == Some(&expected);
        if is_next {
            self.position += 1;
        }
        is_next
    }

    /// Exactly `count` digits as a number.
    fn read_digits(&mut self, count: usize) -> Option<u32> {
        let digits = self.bytes.get(self.position..self.position + count)?;
        if !digits.iter().all(u8::is_ascii_digit) {
            return None;
        }
        self.position += count;
        Some(fold_ascii_digits(digits))
    }

    /// One to nine fraction digits as nanoseconds.
    fn read_fraction(&mut self) -> Option<u32> {
        let start = self.position;
        while self.bytes.get(self.position).is_some_and(u8::is_ascii_digit) && self.position - start < 9 {
            self.position += 1;
        }
        let digits = &self.bytes[start..self.position];
        if digits.is_empty() || self.bytes.get(self.position).is_some_and(u8::is_ascii_digit) {
            return None;
        }
        Some(fold_ascii_digits(digits) * 10_u32.pow(9 - u32::try_from(digits.len()).ok()?))
    }

    /// `Z`, `±HH`, `±HHMM` or `±HH:MM`, up to the end of the text.
    fn read_offset(&mut self) -> Option<FixedOffset> {
        if self.consume_byte(b'Z') {
            return FixedOffset::east_opt(0);
        }
        let rest = std::str::from_utf8(self.bytes.get(self.position..)?).ok()?;
        let offset = parse_offset(rest)?;
        self.position = self.bytes.len();
        Some(offset)
    }

    /// Whether every byte has been read.
    const fn is_at_end(&self) -> bool {
        self.position == self.bytes.len()
    }
}

/// `yyyy[-MM[-dd[THH[:mm[:ss[.fraction]]][offset]]]]`; an offset needs a time.
fn parse_date_optional_time(text: &str) -> Option<ParsedDate> {
    if !text.is_ascii() {
        return None;
    }
    let mut cursor = Cursor {
        bytes: text.as_bytes(),
        position: 0,
    };
    let year = i32::try_from(cursor.read_digits(4)?).ok()?;
    let (mut month, mut day, mut hour, mut minute, mut second, mut nanos) = (1, 1, 0, 0, 0, 0);
    let mut precision = Precision::Year;
    if cursor.consume_byte(b'-') {
        month = cursor.read_digits(2)?;
        precision = Precision::Month;
        if cursor.consume_byte(b'-') {
            day = cursor.read_digits(2)?;
            precision = Precision::Day;
            if cursor.consume_byte(b'T') {
                hour = cursor.read_digits(2)?;
                precision = Precision::Hour;
                if cursor.consume_byte(b':') {
                    minute = cursor.read_digits(2)?;
                    precision = Precision::Minute;
                    if cursor.consume_byte(b':') {
                        second = cursor.read_digits(2)?;
                        precision = Precision::Second;
                        if cursor.consume_byte(b'.') || cursor.consume_byte(b',') {
                            nanos = cursor.read_fraction()?;
                            precision = Precision::Fraction;
                        }
                    }
                }
            }
        }
    }
    let offset = if cursor.is_at_end() {
        None
    } else if matches!(precision, Precision::Year | Precision::Month | Precision::Day) {
        return None;
    } else {
        Some(cursor.read_offset()?)
    };
    if !cursor.is_at_end() {
        return None;
    }
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    let time = NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)?;
    Some(ParsedDate {
        local_time: date.and_time(time),
        precision,
        offset,
    })
}

/// A date-math unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DateUnit {
    /// `y`: a calendar year.
    Year,
    /// `M`: a calendar month.
    Month,
    /// `w`: a week starting on Monday.
    Week,
    /// `d`: a calendar day.
    Day,
    /// `h` or `H`: an hour.
    Hour,
    /// `m`: a minute.
    Minute,
    /// `s`: a second.
    Second,
}

impl DateUnit {
    /// The unit `byte` spells; `None` for any other byte.
    const fn parse(byte: u8) -> Option<Self> {
        match byte {
            b'y' => Some(Self::Year),
            b'M' => Some(Self::Month),
            b'w' => Some(Self::Week),
            b'd' => Some(Self::Day),
            b'h' | b'H' => Some(Self::Hour),
            b'm' => Some(Self::Minute),
            b's' => Some(Self::Second),
            _ => None,
        }
    }
}

/// The unit that opens `math` and what follows it; `None` when `math` opens with no unit.
fn read_date_unit(math: &[u8]) -> Option<(DateUnit, &[u8])> {
    let (&unit_byte, rest) = math.split_first()?;
    Some((DateUnit::parse(unit_byte)?, rest))
}

/// Apply `math` (`+N<unit>`, `-N<unit>`, `/<unit>`, repeated) to `start_millis`.
///
/// # Errors
///
/// `illegal_argument_exception` naming `text` when `math` is malformed or the instant overflows.
fn apply_date_math(
    start_millis: i64,
    math: &str,
    is_rounded_up: bool,
    zone: &TimeZoneSpec,
    text: &str,
) -> Result<i64, OpenSearchError> {
    let mut instant = DateTime::from_timestamp_millis(start_millis).ok_or_else(|| refuse_date(text))?;
    let mut rest = math.as_bytes();
    while let Some((&operator, tail)) = rest.split_first() {
        rest = tail;
        match operator {
            b'/' => {
                let (unit, tail) = read_date_unit(rest).ok_or_else(|| refuse_date(text))?;
                rest = tail;
                instant = round_to_unit(instant, unit, is_rounded_up, zone).ok_or_else(|| refuse_date(text))??;
            }
            b'+' | b'-' => {
                let digit_count = rest.iter().take_while(|byte| byte.is_ascii_digit()).count();
                let amount: i64 = if digit_count == 0 {
                    1
                } else {
                    std::str::from_utf8(&rest[..digit_count])
                        .ok()
                        .and_then(|digits| digits.parse().ok())
                        .ok_or_else(|| refuse_date(text))?
                };
                let (unit, tail) = read_date_unit(&rest[digit_count..]).ok_or_else(|| refuse_date(text))?;
                rest = tail;
                let signed_amount = if operator == b'-' { -amount } else { amount };
                instant = add_units(instant, unit, signed_amount, zone).ok_or_else(|| refuse_date(text))??;
            }
            _ => return Err(refuse_date(text)),
        }
    }
    Ok(instant.timestamp_millis())
}

/// `instant` moved by `amount` units: calendar units in local time of `zone`, time units on the
/// instant. `None` on overflow; `Some(Err)` when the moved local time has no instant in `zone`
/// (see [`resolve_local_time`]).
fn add_units(
    instant: DateTime<Utc>,
    unit: DateUnit,
    amount: i64,
    zone: &TimeZoneSpec,
) -> Option<Result<DateTime<Utc>, OpenSearchError>> {
    // Read in the calendar arms only: a clock unit still moves an instant whose local time
    // `to_local` cannot represent.
    let read_local_time = || zone.to_local(instant);
    let shifted_local_time = match unit {
        DateUnit::Year => shift_months(read_local_time()?, amount.checked_mul(12)?)?,
        DateUnit::Month => shift_months(read_local_time()?, amount)?,
        DateUnit::Week => read_local_time()?.checked_add_signed(TimeDelta::try_weeks(amount)?)?,
        DateUnit::Day => read_local_time()?.checked_add_signed(TimeDelta::try_days(amount)?)?,
        DateUnit::Hour => return instant.checked_add_signed(TimeDelta::try_hours(amount)?).map(Ok),
        DateUnit::Minute => return instant.checked_add_signed(TimeDelta::try_minutes(amount)?).map(Ok),
        DateUnit::Second => return instant.checked_add_signed(TimeDelta::try_seconds(amount)?).map(Ok),
    };
    Some(zone.to_instant(shifted_local_time))
}

/// `local_time` moved by `months`, which may be negative; `None` on overflow.
fn shift_months(local_time: NaiveDateTime, months: i64) -> Option<NaiveDateTime> {
    let magnitude = Months::new(u32::try_from(months.unsigned_abs()).ok()?);
    if months < 0 {
        local_time.checked_sub_months(magnitude)
    } else {
        local_time.checked_add_months(magnitude)
    }
}

/// `instant` rounded to the start of its `unit` in `zone`, or, when `is_rounded_up`, to the last
/// millisecond of that unit. A calendar unit (year, month, week, day) starts at local midnight
/// of its first day; an hour, a minute or a second is cut on the instant (see
/// [`round_to_clock_unit`]). `None` on overflow; `Some(Err)` when the start or the end of a
/// calendar unit has no instant in `zone` (see [`resolve_local_time`]).
fn round_to_unit(
    instant: DateTime<Utc>,
    unit: DateUnit,
    is_rounded_up: bool,
    zone: &TimeZoneSpec,
) -> Option<Result<DateTime<Utc>, OpenSearchError>> {
    let local_time = zone.to_local(instant)?;
    let date = local_time.date();
    let (start_local_time, next_local_time) = match unit {
        DateUnit::Year => {
            let start = NaiveDate::from_ymd_opt(date.year(), 1, 1)?.and_hms_opt(0, 0, 0)?;
            (start, start.checked_add_months(Months::new(12))?)
        }
        DateUnit::Month => {
            let start = NaiveDate::from_ymd_opt(date.year(), date.month(), 1)?.and_hms_opt(0, 0, 0)?;
            (start, start.checked_add_months(Months::new(1))?)
        }
        DateUnit::Week => {
            let monday = date.checked_sub_signed(TimeDelta::days(i64::from(date.weekday().num_days_from_monday())))?;
            let start = monday.and_hms_opt(0, 0, 0)?;
            (start, start.checked_add_signed(TimeDelta::weeks(1))?)
        }
        DateUnit::Day => {
            let start = date.and_hms_opt(0, 0, 0)?;
            (start, start.checked_add_signed(TimeDelta::days(1))?)
        }
        DateUnit::Hour => {
            let seconds_past_start = local_time.minute() * 60 + local_time.second();
            return round_to_clock_unit(
                instant,
                local_time,
                seconds_past_start,
                TimeDelta::hours(1),
                is_rounded_up,
            )
            .map(Ok);
        }
        DateUnit::Minute => {
            return round_to_clock_unit(
                instant,
                local_time,
                local_time.second(),
                TimeDelta::minutes(1),
                is_rounded_up,
            )
            .map(Ok);
        }
        DateUnit::Second => {
            return round_to_clock_unit(instant, local_time, 0, TimeDelta::seconds(1), is_rounded_up).map(Ok);
        }
    };
    if !is_rounded_up {
        return Some(zone.to_instant(start_local_time));
    }
    zone.to_instant(next_local_time)
        .map(|next_instant| next_instant.checked_sub_signed(TimeDelta::milliseconds(1)))
        .transpose()
}

/// `instant` rounded to the start of its hour, minute or second (`length`), or, when
/// `is_rounded_up`, to the last millisecond of it. `seconds_past_start` and the fraction of
/// `local_time` are what the local clock shows past the unit's start, so the start keeps the
/// offset in force at `instant`: an instant in a repeated local hour (the hour a fall-back
/// transition repeats) rounds inside its own occurrence of that hour, which re-reading the local
/// start in the zone would not do. `None` on overflow.
fn round_to_clock_unit(
    instant: DateTime<Utc>,
    local_time: NaiveDateTime,
    seconds_past_start: u32,
    length: TimeDelta,
    is_rounded_up: bool,
) -> Option<DateTime<Utc>> {
    let offset_into_unit =
        TimeDelta::seconds(i64::from(seconds_past_start)) + TimeDelta::nanoseconds(i64::from(local_time.nanosecond()));
    let start = instant.checked_sub_signed(offset_into_unit)?;
    if !is_rounded_up {
        return Some(start);
    }
    start.checked_add_signed(length)?.checked_sub_signed(TimeDelta::milliseconds(1))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use chrono::{DateTime, Days, Utc};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::opensearch::error::render_envelope;

    /// The generated instants outside the transition windows: 1990-01-01T00:00Z up to
    /// 2037-01-01T00:00Z, in microseconds.
    const GENERATED_RANGE_MICROS: std::ops::Range<i64> = 631_152_000_000_000..2_114_380_800_000_000;

    /// How far from a transition a transition-window instant lies, at most: three hours.
    const TRANSITION_WINDOW_MICROS: i64 = 10_800_000_000;

    /// Instants where `America/New_York` changes its offset, in UTC seconds, each derived by hand
    /// from the US rule in force that year: 02:00 local on the first Sunday of April and the last
    /// Sunday of October before 2007, on the second Sunday of March and the first Sunday of
    /// November since.
    const NEW_YORK_TRANSITIONS: [i64; 8] = [
        796_806_000,   // 1995-04-02T07:00Z: 02:00 EST becomes 03:00 EDT.
        814_946_400,   // 1995-10-29T06:00Z: 02:00 EDT becomes 01:00 EST.
        1_772_953_200, // 2026-03-08T07:00Z
        1_793_512_800, // 2026-11-01T06:00Z
        1_805_007_600, // 2027-03-14T07:00Z
        1_825_567_200, // 2027-11-07T06:00Z
        2_088_658_800, // 2036-03-09T07:00Z
        2_109_218_400, // 2036-11-02T06:00Z
    ];

    /// The request instant every case evaluates `now` against.
    fn parse_request_instant() -> DateTime<Utc> {
        "2026-10-08T12:34:56.789Z".parse().expect("a literal instant")
    }

    fn resolve_bound(text: &str, zone: Option<&TimeZoneSpec>, side: BoundSide) -> i64 {
        resolve_date_bound(text, None, zone, parse_request_instant(), side).expect("the bound resolves")
    }

    /// The bounds `text` resolves to at `instant` in `zone`, in microseconds: read down (`gte`)
    /// and up (`lte`), the second keeping values below it.
    fn round_bounds(text: &str, zone: &TimeZoneSpec, instant: DateTime<Utc>) -> (i64, i64) {
        let resolve_side =
            |side| resolve_date_bound(text, None, Some(zone), instant, side).expect("the bound resolves");
        (
            resolve_side(BoundSide::LowerInclusive),
            resolve_side(BoundSide::UpperInclusive),
        )
    }

    const fn spell_unit(unit: DateUnit) -> char {
        match unit {
            DateUnit::Year => 'y',
            DateUnit::Month => 'M',
            DateUnit::Week => 'w',
            DateUnit::Day => 'd',
            DateUnit::Hour => 'h',
            DateUnit::Minute => 'm',
            DateUnit::Second => 's',
        }
    }

    fn expect_unit(zone: &TimeZoneSpec, instant: DateTime<Utc>, amount: u32, unit: DateUnit) -> (i64, i64) {
        match zone {
            TimeZoneSpec::Fixed(offset) => expect_unit_in(offset, instant, amount, unit),
            TimeZoneSpec::Named(named) => expect_unit_in(named, instant, amount, unit),
        }
    }

    /// The hand rule for `now-<amount><unit>/<unit>`: the start and the exclusive end of the unit
    /// that holds the shifted instant, in microseconds. It is written with chrono's calendar API
    /// and integer arithmetic on the local epoch, not with the module's rounding. A calendar unit
    /// steps the local date and starts at local midnight, which every generated zone has exactly
    /// once a day. A clock unit steps the instant and is cut on the local epoch: the instant plus
    /// the offset in force at it.
    fn expect_unit_in<Z: TimeZone>(zone: &Z, instant: DateTime<Utc>, amount: u32, unit: DateUnit) -> (i64, i64) {
        let midnight_micros = |date: NaiveDate| {
            zone.from_local_datetime(&date.and_hms_opt(0, 0, 0).expect("a midnight"))
                .single()
                .expect("one local midnight a day in every generated zone")
                .timestamp_micros()
        };
        let clock_unit = |length_micros: i64| {
            let shifted_micros = instant.timestamp_micros() - length_micros * i64::from(amount);
            let shifted_instant = DateTime::from_timestamp_micros(shifted_micros).expect("a shifted instant");
            let offset_micros = i64::from(
                zone.offset_from_utc_datetime(&shifted_instant.naive_utc())
                    .fix()
                    .local_minus_utc(),
            ) * 1_000_000;
            let start = shifted_micros - (shifted_micros + offset_micros).rem_euclid(length_micros);
            (start, start + length_micros)
        };
        let date = instant.with_timezone(zone).date_naive();
        match unit {
            DateUnit::Year => {
                let start = date.with_ordinal(1).expect("January 1") - Months::new(12 * amount);
                (midnight_micros(start), midnight_micros(start + Months::new(12)))
            }
            DateUnit::Month => {
                let start = date.with_day(1).expect("the first of the month") - Months::new(amount);
                (midnight_micros(start), midnight_micros(start + Months::new(1)))
            }
            DateUnit::Week => {
                let days_back = u64::from(date.weekday().num_days_from_monday()) + 7 * u64::from(amount);
                let start = date - Days::new(days_back);
                (midnight_micros(start), midnight_micros(start + Days::new(7)))
            }
            DateUnit::Day => {
                let start = date - Days::new(u64::from(amount));
                (midnight_micros(start), midnight_micros(start + Days::new(1)))
            }
            DateUnit::Hour => clock_unit(3_600_000_000),
            DateUnit::Minute => clock_unit(60_000_000),
            DateUnit::Second => clock_unit(1_000_000),
        }
    }

    #[test]
    fn generated_instants_round_inside_their_unit_idempotently_and_by_the_calendar_rule() {
        let zones = [
            parse_time_zone("UTC").expect("zone"),
            parse_time_zone("+05:30").expect("offset"),
            parse_time_zone("America/New_York").expect("zone"),
        ];
        let units = [
            DateUnit::Year,
            DateUnit::Month,
            DateUnit::Week,
            DateUnit::Day,
            DateUnit::Hour,
            DateUnit::Minute,
            DateUnit::Second,
        ];
        let mut generator = StdRng::seed_from_u64(0x5EED_DA7E);
        for _ in 0..4_000 {
            // Half the instants lie within three hours of a New York transition, where a gap or
            // a repeated local hour can move a bound.
            let instant_micros = if generator.random_bool(0.5) {
                generator.random_range(GENERATED_RANGE_MICROS)
            } else {
                let transition = NEW_YORK_TRANSITIONS[generator.random_range(0..NEW_YORK_TRANSITIONS.len())];
                transition * 1_000_000 + generator.random_range(-TRANSITION_WINDOW_MICROS..TRANSITION_WINDOW_MICROS)
            };
            let instant = DateTime::from_timestamp_micros(instant_micros).expect("a generated instant");
            let zone = &zones[generator.random_range(0..zones.len())];
            let unit = units[generator.random_range(0..units.len())];
            let symbol = spell_unit(unit);
            let rounding_text = format!("now/{symbol}");
            let (lower_bound, upper_bound) = round_bounds(&rounding_text, zone, instant);
            assert!(
                (lower_bound..upper_bound).contains(&instant_micros),
                "{rounding_text} at {instant} in {zone:?}: [{lower_bound}, {upper_bound})"
            );
            for inside_micros in [lower_bound, upper_bound - 1_000] {
                let inside_instant =
                    DateTime::from_timestamp_micros(inside_micros).expect("an instant inside the unit");
                assert_eq!(
                    round_bounds(&rounding_text, zone, inside_instant),
                    (lower_bound, upper_bound),
                    "{rounding_text} at {instant} in {zone:?}, rounded again from {inside_instant}"
                );
            }
            let amount = generator.random_range(0..=30_u32);
            let shifted_text = format!("now-{amount}{symbol}/{symbol}");
            assert_eq!(
                round_bounds(&shifted_text, zone, instant),
                expect_unit(zone, instant, amount, unit),
                "{shifted_text} at {instant} in {zone:?}"
            );
        }
    }

    #[test]
    fn an_instant_in_the_repeated_new_york_hour_rounds_inside_its_own_occurrence() {
        // On 2026-11-01 New York's clocks fall back from 02:00 EDT to 01:00 EST at 06:00Z, so the
        // local time 01:30:15 occurs at 05:30:15Z (EDT) and again at 06:30:15Z (EST).
        let new_york = parse_time_zone("America/New_York").expect("zone");
        let first_occurrence = DateTime::from_timestamp_micros(1_793_511_015_000_000).expect("05:30:15Z");
        let second_occurrence = DateTime::from_timestamp_micros(1_793_514_615_000_000).expect("06:30:15Z");
        let cases = [
            (
                first_occurrence,
                "now/h",
                (1_793_509_200_000_000, 1_793_512_800_000_000),
                "[05:00Z, 06:00Z)",
            ),
            (
                second_occurrence,
                "now/h",
                (1_793_512_800_000_000, 1_793_516_400_000_000),
                "[06:00Z, 07:00Z)",
            ),
            (
                second_occurrence,
                "now/m",
                (1_793_514_600_000_000, 1_793_514_660_000_000),
                "[06:30Z, 06:31Z)",
            ),
        ];
        for (instant, text, expected, meaning) in cases {
            assert_eq!(
                round_bounds(text, &new_york, instant),
                expected,
                "{text} at {instant}: {meaning}"
            );
        }
    }

    #[test]
    fn a_local_time_in_a_gap_moves_forward_and_a_repeated_one_takes_the_earlier_instant() {
        let new_york = parse_time_zone("America/New_York").expect("zone");
        // 2026-03-08: 02:00 EST jumps to 03:00 EDT at 07:00Z, so 02:30 does not exist; moved
        // forward by the hour it falls in, it reads 03:30 EDT, which is 07:30Z.
        assert_eq!(
            resolve_bound("2026-03-08T02:30:00", Some(&new_york), BoundSide::LowerInclusive),
            1_772_955_000_000_000
        );
        // 2026-11-01: 01:30 occurs at 05:30Z (EDT) and 06:30Z (EST); the earlier one counts.
        assert_eq!(
            resolve_bound("2026-11-01T01:30:00", Some(&new_york), BoundSide::LowerInclusive),
            1_793_511_000_000_000
        );
    }

    #[test]
    fn date_math_resolves_now_relative_bounds_with_rounding_per_side() {
        let dubai = parse_time_zone("Asia/Dubai").expect("zone");
        let berlin = parse_time_zone("Europe/Berlin").expect("zone");
        let cases: [(&str, Option<&TimeZoneSpec>, BoundSide, i64, &str); 17] = [
            (
                "now",
                None,
                BoundSide::LowerInclusive,
                1_791_462_896_789_000,
                "2026-10-08T12:34:56.789Z",
            ),
            (
                "now-90d",
                None,
                BoundSide::LowerInclusive,
                1_783_686_896_789_000,
                "2026-07-10T12:34:56.789Z",
            ),
            (
                "now/d",
                None,
                BoundSide::LowerInclusive,
                1_791_417_600_000_000,
                "2026-10-08T00:00Z",
            ),
            (
                "now/d",
                None,
                BoundSide::UpperExclusive,
                1_791_417_600_000_000,
                "< 2026-10-08T00:00Z",
            ),
            (
                "now/d",
                None,
                BoundSide::UpperInclusive,
                1_791_504_000_000_000,
                "< 2026-10-09T00:00Z (23:59:59.999 + 1 ms)",
            ),
            (
                "now/d",
                None,
                BoundSide::LowerExclusive,
                1_791_504_000_000_000,
                ">= 2026-10-09T00:00Z (23:59:59.999 + 1 ms)",
            ),
            (
                "now-3d/d",
                Some(&dubai),
                BoundSide::LowerInclusive,
                1_791_144_000_000_000,
                "2026-10-05T00:00+04 = 2026-10-04T20:00Z",
            ),
            (
                "now-3d/d",
                Some(&dubai),
                BoundSide::UpperInclusive,
                1_791_230_400_000_000,
                "2026-10-06T00:00+04 = 2026-10-05T20:00Z",
            ),
            (
                "now/d",
                Some(&berlin),
                BoundSide::LowerInclusive,
                1_791_410_400_000_000,
                "2026-10-08T00:00+02 = 2026-10-07T22:00Z",
            ),
            (
                "now/w",
                None,
                BoundSide::LowerInclusive,
                1_791_158_400_000_000,
                "Monday 2026-10-05T00:00Z",
            ),
            (
                "now/w",
                None,
                BoundSide::UpperInclusive,
                1_791_763_200_000_000,
                "< Monday 2026-10-12T00:00Z",
            ),
            (
                "now/M",
                None,
                BoundSide::LowerInclusive,
                1_790_812_800_000_000,
                "2026-10-01T00:00Z",
            ),
            (
                "now/M",
                None,
                BoundSide::UpperInclusive,
                1_793_491_200_000_000,
                "< 2026-11-01T00:00Z",
            ),
            (
                "now+1h/h",
                None,
                BoundSide::LowerInclusive,
                1_791_464_400_000_000,
                "2026-10-08T13:00Z",
            ),
            (
                "now/H",
                None,
                BoundSide::LowerInclusive,
                1_791_460_800_000_000,
                "2026-10-08T12:00Z: `H` spells an hour like `h`",
            ),
            (
                "now+d",
                None,
                BoundSide::LowerInclusive,
                1_791_549_296_789_000,
                "2026-10-09T12:34:56.789Z: no digits mean one unit",
            ),
            (
                "2026-10-08||-1M/d",
                None,
                BoundSide::LowerInclusive,
                1_788_825_600_000_000,
                "2026-09-08T00:00Z",
            ),
        ];
        for (text, zone, side, expected, meaning) in cases {
            assert_eq!(resolve_bound(text, zone, side), expected, "{text} {side:?}: {meaning}");
        }
    }

    #[test]
    fn date_text_bounds_round_the_fields_they_omit_per_side() {
        let india = parse_time_zone("+05:30").expect("offset");
        let cases: [(&str, Option<&TimeZoneSpec>, BoundSide, i64, &str); 9] = [
            (
                "2026-10-08",
                None,
                BoundSide::LowerInclusive,
                1_791_417_600_000_000,
                "2026-10-08T00:00Z",
            ),
            (
                "2026-10-08",
                None,
                BoundSide::UpperInclusive,
                1_791_504_000_000_000,
                "< 2026-10-09T00:00Z",
            ),
            (
                "2026",
                None,
                BoundSide::UpperInclusive,
                1_798_761_600_000_000,
                "< 2027-01-01T00:00Z",
            ),
            (
                "2026-10",
                None,
                BoundSide::LowerInclusive,
                1_790_812_800_000_000,
                "2026-10-01T00:00Z",
            ),
            (
                "2026-10",
                None,
                BoundSide::UpperInclusive,
                1_793_491_200_000_000,
                "< 2026-11-01T00:00Z",
            ),
            (
                "2026-10-08T12",
                None,
                BoundSide::UpperInclusive,
                1_791_464_400_000_000,
                "< 13:00Z",
            ),
            (
                "2026-10-08T12:34",
                None,
                BoundSide::UpperInclusive,
                1_791_462_900_000_000,
                "< 12:35Z",
            ),
            (
                "2026-10-08T12:34:56Z",
                None,
                BoundSide::UpperInclusive,
                1_791_462_897_000_000,
                "< 12:34:57.000Z",
            ),
            (
                "2026-10-08",
                Some(&india),
                BoundSide::LowerInclusive,
                1_791_397_800_000_000,
                "2026-10-08T00:00+05:30 = 2026-10-07T18:30Z",
            ),
        ];
        for (text, zone, side, expected, meaning) in cases {
            assert_eq!(resolve_bound(text, zone, side), expected, "{text} {side:?}: {meaning}");
        }
        assert_eq!(
            resolve_bound("2026-10-08T12:34:56.789Z", None, BoundSide::UpperInclusive),
            1_791_462_896_790_000,
            "a stated millisecond rounds up to itself"
        );
    }

    #[test]
    fn a_stated_offset_wins_over_the_time_zone_and_a_comma_introduces_a_fraction() {
        let india = parse_time_zone("+05:30").expect("offset");

        assert_eq!(
            resolve_bound("2026-10-08T12:34:56.789+04:00", Some(&india), BoundSide::LowerInclusive),
            1_791_448_496_789_000,
            "08:34:56.789Z"
        );
        assert_eq!(
            resolve_bound("2026-10-08T12:34:56,789Z", None, BoundSide::LowerInclusive),
            1_791_462_896_789_000,
            "the same instant as with a dot"
        );
    }

    /// The instants the refusals of `malformed_dates_and_date_math_are_refused` start from, zoned
    /// or not, and one clock step past such a start. Each resolves, so that test refuses a later
    /// step and not the start. A year step here reads a local time in range and a clock step reads
    /// none: 13 minutes remain before the last instant `chrono` represents (`8210266876799999` ms),
    /// so `+1m` from there stands where `/d` is refused.
    #[test]
    fn an_instant_near_the_representable_limits_resolves_while_every_local_time_it_reads_stays_in_range() {
        let east_of_utc = parse_time_zone("+18:00").expect("offset");
        let west_of_utc = parse_time_zone("-18:00").expect("offset");
        let cases: [(&str, Option<&TimeZoneSpec>, i64, &str); 6] = [
            (
                "now+260116y",
                None,
                8_210_259_578_096_789_000,
                "262142-10-08T12:34:56.789Z",
            ),
            (
                "now+260116y+2020h",
                Some(&east_of_utc),
                8_210_266_850_096_789_000,
                "262142-12-31T16:34:56.789Z",
            ),
            (
                "now-264169y",
                Some(&east_of_utc),
                -8_334_576_991_503_211_000,
                "-262143-10-08T12:34:56.789Z",
            ),
            (
                "8210266876000000||",
                Some(&east_of_utc),
                8_210_266_876_000_000_000,
                "the anchor alone",
            ),
            (
                "8210266876000000||+1m",
                Some(&east_of_utc),
                8_210_266_876_060_000_000,
                "one minute on the instant",
            ),
            (
                "8210266800000000||",
                Some(&west_of_utc),
                8_210_266_800_000_000_000,
                "the anchor alone",
            ),
        ];
        for (text, zone, expected, meaning) in cases {
            assert_eq!(
                resolve_bound(text, zone, BoundSide::LowerInclusive),
                expected,
                "{text}: {meaning}"
            );
        }
    }

    #[test]
    fn the_named_date_formats_accept_their_own_spelling() {
        let resolve_formatted = |text, format| {
            resolve_date_bound(
                text,
                Some(format),
                None,
                parse_request_instant(),
                BoundSide::LowerInclusive,
            )
            .expect("the bound resolves")
        };

        assert_eq!(
            resolve_formatted("2026-10-08T12:34:56.789Z", DateFormat::DateTime),
            1_791_462_896_789_000
        );
        assert_eq!(
            resolve_formatted("2026-10-08T12:34:56.789Z", DateFormat::StrictDateOptionalTime),
            1_791_462_896_789_000
        );
        assert_eq!(
            resolve_formatted("2026-10-08", DateFormat::StrictDateOptionalTime),
            1_791_417_600_000_000
        );
    }

    #[test]
    fn epoch_millis_bounds_take_the_millisecond_meaning() {
        let resolve_epoch_bound = |format, side| {
            resolve_date_bound("1700000000000", format, None, parse_request_instant(), side).expect("bound")
        };
        for format in [Some(DateFormat::EpochMillis), None] {
            assert_eq!(
                resolve_epoch_bound(format, BoundSide::LowerInclusive),
                1_700_000_000_000_000,
                "gte X keeps >= X·1000"
            );
            assert_eq!(
                resolve_epoch_bound(format, BoundSide::LowerExclusive),
                1_700_000_000_001_000,
                "gt X keeps >= (X+1)·1000"
            );
            assert_eq!(
                resolve_epoch_bound(format, BoundSide::UpperInclusive),
                1_700_000_000_001_000,
                "lte X keeps < (X+1)·1000"
            );
            assert_eq!(
                resolve_epoch_bound(format, BoundSide::UpperExclusive),
                1_700_000_000_000_000,
                "lt X keeps < X·1000"
            );
        }
        assert_eq!(parse_stored_date_millis("1700000000000"), Some(1_700_000_000_000));
        assert_eq!(
            parse_stored_date_millis("2026-10-08T12:34:56.789123Z"),
            Some(1_791_462_896_789)
        );
    }

    #[tokio::test]
    async fn time_zones_parse_as_iana_names_or_offsets() {
        let fixed_offset = |seconds| TimeZoneSpec::Fixed(FixedOffset::east_opt(seconds).expect("offset"));
        assert_eq!(
            parse_time_zone("Europe/Berlin").expect("zone"),
            TimeZoneSpec::Named(chrono_tz::Europe::Berlin)
        );
        assert_eq!(
            parse_time_zone("UTC").expect("zone"),
            TimeZoneSpec::Named(chrono_tz::UTC)
        );
        assert_eq!(parse_time_zone("+05:30").expect("offset"), fixed_offset(19_800));
        assert_eq!(parse_time_zone("-08:00").expect("offset"), fixed_offset(-28_800));
        assert_eq!(parse_time_zone("+0530").expect("offset"), fixed_offset(19_800));
        assert_eq!(parse_time_zone("-03").expect("offset"), fixed_offset(-10_800));
        assert_eq!(
            parse_time_zone("+18:00").expect("offset"),
            fixed_offset(64_800),
            "the largest offset"
        );
        assert_eq!(
            parse_time_zone("-18:00").expect("offset"),
            fixed_offset(-64_800),
            "the smallest offset"
        );
        for refused_zone in [
            "Mars/Base",
            "+25:00",
            "+18:01",
            "-18:01",
            "+05:60",
            "05:30",
            "+5:30",
            "",
            "+０５:30",
        ] {
            let (status, body) = render_envelope(parse_time_zone(refused_zone).expect_err("refused")).await;
            assert_eq!(
                (status, body["error"]["type"].clone()),
                (StatusCode::BAD_REQUEST, serde_json::json!("parsing_exception")),
                "{refused_zone:?}"
            );
        }
    }

    async fn assert_date_refused(text: &str, format: Option<DateFormat>, zone: Option<&TimeZoneSpec>) {
        let error =
            resolve_date_bound(text, format, zone, parse_request_instant(), BoundSide::LowerInclusive).expect_err(text);
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception", "{text}");
    }

    #[tokio::test]
    async fn malformed_dates_and_date_math_are_refused() {
        let refused_dates = [
            ("2026-13-01", None),
            ("2026-02-30", None),
            ("yesterday", None),
            ("now-3x", None),
            ("now-", None),
            ("now/", None),
            ("now/x", None),
            ("now+1", None),
            ("now*1d", None),
            ("2026-10-08||+1d||", None),
            ("1700000000000.5", Some(DateFormat::EpochMillis)),
            ("2026-10-08", Some(DateFormat::EpochMillis)),
            ("2026-10-08T12:00:00Z", Some(DateFormat::DateTime)),
            ("2026-10-08T12:00:00.000", Some(DateFormat::DateTime)),
            ("2026-10-08Z", Some(DateFormat::StrictDateOptionalTime)),
            ("２０２６", None),
            // An offset needs its sign.
            ("2026-10-08T12:00:00x05:00", None),
            ("2026-10-08T12:00:00.1234567890Z", None),
            // A separator promises a fraction.
            ("2026-10-08T12:34:56.Z", None),
            // Each of these overflows a different step of the math.
            ("now+99999999999999999999d", None),
            ("now+9223372036854775807y", None),
            ("now+9223372036854775807M", None),
            ("now+9223372036854775807w", None),
            ("now+9223372036854775807d", None),
            ("now+9223372036854775807h", None),
            ("now+9223372036854775807m", None),
            ("now+9223372036854775807s", None),
            ("now-300000y", None),
            ("9223372036854775807", Some(DateFormat::EpochMillis)),
            ("9223372036854775807||+1d", None),
            // The year holds, but the unit's end is the first instant past the largest date.
            ("now+260116y/y", None),
        ];
        for (text, format) in refused_dates {
            assert_date_refused(text, format, None).await;
        }
        assert_eq!(parse_stored_date_millis("not a date"), None);

        let east_of_utc = parse_time_zone("+18:00").expect("offset");
        let west_of_utc = parse_time_zone("-18:00").expect("offset");
        let refused_zoned_dates = [
            // Rounding and calendar math read the instant on the local clock, past the largest date.
            ("now+260116y+2020h/d", &east_of_utc),
            ("now+260116y+2020h+1d", &east_of_utc),
            ("8210266876000000||/d", &east_of_utc),
            ("8210266876000000||+1d", &east_of_utc),
            // The local time holds but no instant does: the instant lies past the largest date.
            ("8210266800000000||+1d", &west_of_utc),
            // The same before the smallest date, where the day before it does not exist either.
            ("now-264169y/y", &east_of_utc),
        ];
        for (text, zone) in refused_zoned_dates {
            assert_date_refused(text, None, Some(zone)).await;
        }
    }

    #[tokio::test]
    async fn an_unknown_date_format_is_a_parsing_exception_naming_it() {
        let (status, body) = render_envelope(DateFormat::parse("yyyy-MM-dd").expect_err("refused")).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "parsing_exception");
        assert!(
            body["error"]["reason"].as_str().expect("reason").contains("yyyy-MM-dd"),
            "{body}"
        );
    }
}
