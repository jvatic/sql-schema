/*!
Parse a migration path into a [PathTemplate] to later resolve the name of a new migration being written.
*/

pub use ast::{PathTemplate, Semver, TemplateData, UpDown};
pub use chrono::{DateTime, Utc};
pub use parser::ParseError;

mod parser {
    use std::{cmp::Ordering, ops::Range};

    use chrono::NaiveDate;
    use thiserror::Error;
    use winnow::{
        ascii::digit1,
        combinator::{alt, fail, opt, repeat, separated},
        error::{StrContext, StrContextValue},
        stream::AsChar,
        token::{take_until, take_while},
        Parser, Result,
    };

    use super::{
        ast::{
            Date, DateTime, DoUndo, EpochTimestamp, PaddedNumber, Segment, SegmentKind, Semver,
            SubSecond, Time, Timestamp, Token,
        },
        PathTemplate, UpDown,
    };

    #[derive(Error, Debug)]
    pub struct ParseError {
        message: String,
        span: Range<usize>,
        input: String,
    }

    impl std::fmt::Display for ParseError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let lines = self.message.split('\n').collect::<Vec<_>>();
            let m1 = lines[0];
            let m2 = lines.get(1).copied().unwrap_or(m1);
            let title =
                format!("Oops, we couldn't sort out you're migration naming convention: {m1}");
            let message = annotate_snippets::Level::Error.title(&title).snippet(
                annotate_snippets::Snippet::source(&self.input)
                    .fold(true)
                    .annotation(
                        annotate_snippets::Level::Error
                            .span(self.span.clone())
                            .label(m2),
                    ),
            );
            let renderer = annotate_snippets::Renderer::plain();
            let rendered = renderer.render(message);
            rendered.fmt(f)
        }
    }

    fn digit_n<'i>(n: usize) -> impl FnMut(&mut &'i str) -> Result<&'i str> {
        move |input: &mut &'i str| take_while(n, AsChar::is_dec_digit).parse_next(input)
    }

    fn dot(input: &mut &str) -> Result<Token> {
        ".".take().value(Token::Dot).parse_next(input)
    }

    fn underscore(input: &mut &str) -> Result<Token> {
        "_".take().value(Token::Underscore).parse_next(input)
    }

    fn dash(input: &mut &str) -> Result<Token> {
        "-".take().value(Token::Dash).parse_next(input)
    }

    fn sep(input: &mut &str) -> Result<Token> {
        alt((dot, underscore, dash)).parse_next(input)
    }

    fn padded_number(input: &mut &str) -> Result<Token> {
        digit1
            .take()
            .parse_to::<PaddedNumber>()
            .map(Token::PaddedNumber)
            .parse_next(input)
    }

    fn random_number(input: &mut &str) -> Result<Token> {
        digit1
            .take()
            .parse_to::<usize>()
            .map(Token::RandomNumber)
            .parse_next(input)
    }

    fn semver(input: &mut &str) -> Result<Token> {
        separated(3, digit1, '.')
            .map(|_: Vec<&str>| ()) // TODO: why is this map needed?
            .take()
            .parse_to::<Semver>()
            .map(Token::Semver)
            .parse_next(input)
    }

    /// timestamps should all be after the year 2000
    const MIN_DATE: NaiveDate = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    /// timestamps should all be before the year 2100
    const MAX_DATE: NaiveDate = NaiveDate::from_ymd_opt(2100, 1, 1).unwrap();

    fn datetime(input: &mut &str) -> Result<Token> {
        fn year(input: &mut &str) -> Result<i32> {
            ("20", digit_n(2))
                .take()
                .parse_to::<i32>()
                .parse_next(input)
        }

        fn month(input: &mut &str) -> Result<u32> {
            digit_n(2)
                .parse_to::<u32>()
                .verify(|mm| *mm <= 12 && *mm > 0)
                .parse_next(input)
        }

        fn day(input: &mut &str) -> Result<u32> {
            digit_n(2)
                .parse_to::<u32>()
                .verify(|dd| *dd <= 31 && *dd > 0)
                .parse_next(input)
        }

        fn hour(input: &mut &str) -> Result<u32> {
            digit_n(2)
                .parse_to::<u32>()
                .verify(|hh| *hh <= 12 && *hh > 0)
                .parse_next(input)
        }

        fn minute(input: &mut &str) -> Result<u32> {
            digit_n(2)
                .parse_to::<u32>()
                .verify(|mm| *mm < 60)
                .parse_next(input)
        }

        fn second(input: &mut &str) -> Result<u32> {
            digit_n(2)
                .parse_to::<u32>()
                .verify(|ss| *ss < 60)
                .parse_next(input)
        }

        fn submilli(input: &mut &str) -> Result<SubSecond> {
            take_while(1..=3, AsChar::is_dec_digit)
                .parse_to::<u32>()
                .map(SubSecond::Milli)
                .parse_next(input)
        }

        fn submicro(input: &mut &str) -> Result<SubSecond> {
            take_while(6, AsChar::is_dec_digit)
                .parse_to::<u32>()
                .map(SubSecond::Micro)
                .parse_next(input)
        }

        fn subnano(input: &mut &str) -> Result<SubSecond> {
            take_while(9, AsChar::is_dec_digit)
                .parse_to::<u32>()
                .map(SubSecond::Nano)
                .parse_next(input)
        }

        fn sep_literal<'i>(input: &mut &'i str) -> Result<&'i str> {
            sep.take().parse_next(input)
        }

        fn time(input: &mut &str) -> Result<Time> {
            (
                hour,
                opt(sep_literal),
                minute,
                opt((
                    opt(sep_literal),
                    second,
                    opt((opt(sep_literal), alt((subnano, submicro, submilli)))),
                )),
            )
                .map(|(hour, s1, minute, second)| {
                    let hour_sep = s1.map(|s| s.to_string());
                    let (minute_sep, second, second_sep, subsecond) =
                        if let Some((s2, second, subsec)) = second {
                            let minute_sep = s2.map(|s| s.to_string());

                            let (second_sep, subsecond) = if let Some((s3, subsecond)) = subsec {
                                let second_sep = s3.map(|s| s.to_string());
                                (second_sep, Some(subsecond))
                            } else {
                                (None, None)
                            };

                            (minute_sep, Some(second), second_sep, subsecond)
                        } else {
                            (None, None, None, None)
                        };

                    Time {
                        hour,
                        hour_sep,
                        minute,
                        minute_sep,
                        second,
                        second_sep,
                        subsecond,
                    }
                })
                .parse_next(input)
        }

        (
            year,
            opt(sep_literal),
            month,
            opt(sep_literal),
            day,
            opt((opt(sep_literal), time)),
        )
            .map(|(year, s1, month, s2, day, time_or_rand)| {
                let year_sep = s1.map(|s| s.to_string());
                let month_sep = s2.map(|s| s.to_string());
                let date = Date {
                    year,
                    year_sep,
                    month,
                    month_sep,
                    day,
                };

                let (date_sep, time) = if let Some((s3, time)) = time_or_rand {
                    let date_sep = s3.map(|s| s.to_string());
                    (date_sep, Some(time))
                } else {
                    (None, None)
                };

                Token::Timestamp(Timestamp::DateTime(DateTime {
                    date,
                    date_sep,
                    time,
                }))
            })
            .parse_next(input)
    }

    fn validate_datetime<Z: chrono::TimeZone>(
        ts: chrono::DateTime<Z>,
    ) -> Option<chrono::DateTime<Z>> {
        if matches!(
            ts.date_naive().cmp(&MIN_DATE),
            Ordering::Greater | Ordering::Equal
        ) && matches!(
            ts.date_naive().cmp(&MAX_DATE),
            Ordering::Less | Ordering::Equal
        ) {
            Some(ts)
        } else {
            None
        }
    }

    fn epoch_seconds(input: &mut &str) -> Result<EpochTimestamp> {
        digit1
            .take()
            .parse_to::<i64>()
            .verify_map(|secs| chrono::DateTime::from_timestamp(secs, 0))
            .verify_map(validate_datetime)
            .map(|ts| ts.timestamp())
            .map(EpochTimestamp::Second)
            .parse_next(input)
    }

    fn epoch_millis(input: &mut &str) -> Result<EpochTimestamp> {
        digit1
            .take()
            .parse_to::<i64>()
            .verify_map(chrono::DateTime::from_timestamp_millis)
            .verify_map(validate_datetime)
            .map(|ts| ts.timestamp_millis())
            .map(EpochTimestamp::Milli)
            .parse_next(input)
    }

    fn epoch_micros(input: &mut &str) -> Result<EpochTimestamp> {
        digit1
            .take()
            .parse_to::<i64>()
            .verify_map(chrono::DateTime::from_timestamp_micros)
            .verify_map(validate_datetime)
            .map(|ts| ts.timestamp_micros())
            .map(EpochTimestamp::Micro)
            .parse_next(input)
    }

    fn epoch_nanos(input: &mut &str) -> Result<EpochTimestamp> {
        digit1
            .take()
            .parse_to::<i64>()
            .map(chrono::DateTime::from_timestamp_nanos)
            .verify_map(validate_datetime)
            .verify_map(|ts| ts.timestamp_nanos_opt())
            .map(EpochTimestamp::Nano)
            .parse_next(input)
    }

    fn epoch_timestamp(input: &mut &str) -> Result<Token> {
        alt((epoch_nanos, epoch_micros, epoch_millis, epoch_seconds))
            .map(Timestamp::Epoch)
            .map(Token::Timestamp)
            .parse_next(input)
    }

    fn name(input: &mut &str) -> Result<Token> {
        take_until(1.., '.')
            .map(|s: &str| Token::Name(s.to_owned()))
            .context(StrContext::Label("name"))
            .context(StrContext::Expected(StrContextValue::Description(
                "name not to contain `.`s",
            )))
            .parse_next(input)
    }

    fn updown(input: &mut &str) -> Result<Token> {
        alt((
            "down".value(Token::UpDown(UpDown::Down)),
            "undo".value(Token::DoUndo(DoUndo::Undo)),
            "up".value(Token::UpDown(UpDown::Up)),
            "do".value(Token::DoUndo(DoUndo::Do)),
        ))
        .context(StrContext::Label("updown"))
        .context(StrContext::Expected(StrContextValue::StringLiteral("up")))
        .context(StrContext::Expected(StrContextValue::StringLiteral("down")))
        .context(StrContext::Expected(StrContextValue::StringLiteral("do")))
        .context(StrContext::Expected(StrContextValue::StringLiteral("undo")))
        .parse_next(input)
    }

    fn prefix(input: &mut &str) -> Result<Token> {
        fn z_prefix<'i>(input: &mut &'i str) -> Result<&'i str> {
            take_while(0.., |c| c == 'z' || c == 'Z').parse_next(input)
        }

        fn v_prefix<'i>(input: &mut &'i str) -> Result<&'i str> {
            alt(('v', 'V')).take().parse_next(input)
        }

        (z_prefix, v_prefix)
            .take()
            .map(|s: &str| Token::Prefix(s.to_owned()))
            .parse_next(input)
    }

    fn number(input: &mut &str) -> Result<Vec<Token>> {
        (
            alt((
                datetime,
                epoch_timestamp,
                semver,
                padded_number,
                fail.context(StrContext::Label("number"))
                    .context(StrContext::Expected(StrContextValue::Description(
                        "datetime",
                    )))
                    .context(StrContext::Expected(StrContextValue::Description(
                        "epoch timestamp",
                    )))
                    .context(StrContext::Expected(StrContextValue::Description(
                        "padded number",
                    )))
                    .context(StrContext::Expected(StrContextValue::Description("semver"))),
            )),
            opt((
                repeat(0.., sep).map(|t: Vec<_>| t),
                alt((epoch_timestamp, random_number)),
            )),
        )
            .map(|(t1, t2)| {
                let mut tokens = vec![Some(t1)];

                if let Some((s, t)) = t2 {
                    s.into_iter().for_each(|s| tokens.push(Some(s)));
                    tokens.push(Some(t));
                }

                tokens.into_iter().flatten().collect()
            })
            .parse_next(input)
    }

    fn dir_ident(input: &mut &str) -> Result<Segment> {
        (
            opt(prefix),
            number,
            opt((repeat(1.., sep).map(|t: Vec<_>| t), name)),
        )
            .map(|(prefix, number, name)| {
                let mut children = vec![prefix];
                number.into_iter().for_each(|s| children.push(Some(s)));

                if let Some((sep, name)) = name {
                    sep.into_iter().for_each(|s| children.push(Some(s)));
                    children.push(Some(name));
                }

                let tokens = children.into_iter().flatten().collect();

                Segment {
                    kind: SegmentKind::Dir,
                    tokens,
                }
            })
            .parse_next(input)
    }

    fn file_ext(input: &mut &str) -> Result<Token> {
        ".sql"
            .value(Token::Extension)
            .context(StrContext::Label("file ext"))
            .context(StrContext::Expected(StrContextValue::StringLiteral(".sql")))
            .parse_next(input)
    }

    fn file_nonident(input: &mut &str) -> Result<Segment> {
        (updown, file_ext)
            .map(|(updown, ext)| Segment {
                kind: SegmentKind::File,
                tokens: vec![updown, ext],
            })
            .parse_next(input)
    }

    fn file_ident(input: &mut &str) -> Result<Segment> {
        (
            opt(prefix),
            number,
            opt((repeat(0.., sep).map(|t: Vec<_>| t), name)),
            opt((dot, updown)),
            file_ext,
        )
            .map(|(prefix, number, name, updown, ext)| {
                let mut children = vec![prefix];
                number.into_iter().for_each(|s| children.push(Some(s)));

                if let Some((sep, name)) = name {
                    sep.into_iter().for_each(|s| children.push(Some(s)));
                    children.push(Some(name));
                }

                if let Some((sep, updown)) = updown {
                    children.push(Some(sep));
                    children.push(Some(updown));
                }

                children.push(Some(ext));

                let tokens = children.into_iter().flatten().collect();

                Segment {
                    kind: SegmentKind::File,
                    tokens,
                }
            })
            .parse_next(input)
    }

    fn path_sep<'i>(input: &mut &'i str) -> Result<&'i str> {
        alt(('/', '\\')).take().parse_next(input)
    }

    fn path(input: &mut &str) -> Result<Vec<Segment>> {
        alt((
            (dir_ident, path_sep, file_nonident).map(|(dir, _sep, file)| vec![dir, file]),
            file_ident.map(|file| vec![file]),
        ))
        .parse_next(input)
    }

    pub fn parse(input: &str) -> std::result::Result<PathTemplate, ParseError> {
        let segments = path.parse(input).map_err(|e| ParseError {
            message: e.inner().to_string(),
            span: e.char_span(),
            input: input.to_owned(),
        })?;

        Ok(PathTemplate { segments })
    }
}

mod ast {
    use std::{fmt, str::FromStr};

    use anyhow::anyhow;
    use chrono::Utc;

    use super::parser::{self, ParseError};

    #[derive(Debug, PartialEq)]
    pub struct PathTemplate {
        pub(crate) segments: Vec<Segment>,
    }

    impl PathTemplate {
        pub fn parse(path: &str) -> Result<Self, ParseError> {
            parser::parse(path)
        }

        pub fn includes_up_down(&self) -> bool {
            self.segments.iter().any(|s| {
                s.tokens
                    .iter()
                    .rev()
                    .any(|t| matches!(t, Token::UpDown(_) | Token::DoUndo(_)))
            })
        }

        pub fn with_up_down(self) -> Self {
            let mut segments = self.segments;
            if let Some(s) = segments.last_mut() {
                let ext = s.tokens.pop().unwrap_or(Token::Extension);
                if !matches!(
                    s.tokens.last(),
                    Some(Token::UpDown(_)) | Some(Token::DoUndo(_))
                ) {
                    s.tokens.push(Token::Dot);
                    s.tokens.push(Token::UpDown(UpDown::Up));
                }
                s.tokens.push(ext);
            }
            Self { segments }
        }

        pub fn resolve(&self, data: &TemplateData) -> String {
            super::resolver::Resolve::resolve(self, data)
        }
    }

    impl Default for PathTemplate {
        fn default() -> Self {
            Self {
                segments: vec![Segment {
                    kind: SegmentKind::File,
                    tokens: vec![
                        Token::Timestamp(Timestamp::Epoch(EpochTimestamp::Second(0))),
                        Token::Underscore,
                        Token::Name("generated_migration".to_string()),
                        Token::Dot,
                        Token::UpDown(UpDown::Up),
                        Token::Extension,
                    ],
                }],
            }
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct Segment {
        pub kind: SegmentKind,
        pub tokens: Vec<Token>,
    }

    #[derive(Debug, PartialEq)]
    pub enum SegmentKind {
        Dir,
        File,
    }

    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct TemplateData {
        pub timestamp: chrono::DateTime<Utc>,
        pub name: String,
        pub up_down: Option<UpDown>,
        pub counter: Option<usize>,
        pub random: Option<usize>,
        pub semver: Option<Semver>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum Token {
        /// e.g. "V"
        Prefix(String),
        /// padded number e.g. 0001, 0002, etc.
        PaddedNumber(PaddedNumber),
        /// any sequence of numbers
        RandomNumber(usize),
        /// e.g. 0.1.0, 11.12.13, etc
        Semver(Semver),
        /// represents a date/time
        Timestamp(Timestamp),
        /// name of the migration
        Name(String),
        /// either ".up" or ".down"
        UpDown(UpDown),
        /// either ".do" or ".undo" (alias for UpDown)
        DoUndo(DoUndo),
        /// literal underscore ("_")
        Underscore,
        /// literal dot (".")
        Dot,
        /// literal dash ("-")
        Dash,
        /// file extension (e.g. ".sql")
        Extension,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct PaddedNumber {
        pub width: usize,
        pub number: usize,
    }

    impl FromStr for PaddedNumber {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let width = s.len();
            let number = s.parse::<usize>()?;

            Ok(Self { width, number })
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum UpDown {
        Up,
        Down,
    }

    impl FromStr for UpDown {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            Ok(match s {
                "up" => Self::Up,
                "down" => Self::Down,
                _ => return Err(anyhow!("invalid UP_DOWN token: {:?}", s)),
            })
        }
    }

    impl From<DoUndo> for UpDown {
        fn from(value: DoUndo) -> Self {
            match value {
                DoUndo::Do => Self::Up,
                DoUndo::Undo => Self::Down,
            }
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum DoUndo {
        Do,
        Undo,
    }

    impl FromStr for DoUndo {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            Ok(match s {
                "do" => Self::Do,
                "undo" => Self::Undo,
                _ => return Err(anyhow!("invalid DO_UNDO token: {:?}", s)),
            })
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct Semver {
        major: u32,
        minor: u32,
        patch: u32,
        widths: (usize, usize, usize),
    }

    impl Semver {
        pub fn increment_minor(self) -> Self {
            Self {
                minor: self.minor + 1,
                patch: 0,
                ..self
            }
        }
    }

    impl fmt::Display for Semver {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let (w1, w2, w3) = self.widths;
            write!(
                f,
                "{:0>w1$}.{:0>w2$}.{:0>w3$}",
                self.major, self.minor, self.patch
            )
        }
    }

    impl FromStr for Semver {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let parts = s
                .splitn(3, '.')
                .map(|s| {
                    let width = s.len();
                    let num = s.parse::<u32>()?;
                    Ok::<_, anyhow::Error>((width, num))
                })
                .collect::<Result<Vec<_>, _>>()?;

            if parts.len() != 3 {
                return Err(anyhow!("invalid semver: {s}"));
            }

            Ok(Self {
                major: parts[0].1,
                minor: parts[1].1,
                patch: parts[2].1,
                widths: (parts[0].0, parts[1].0, parts[2].0),
            })
        }
    }

    impl Default for Semver {
        fn default() -> Self {
            Self {
                major: 0,
                minor: 1,
                patch: 0,
                widths: (6, 6, 2),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum Timestamp {
        Epoch(EpochTimestamp),
        DateTime(DateTime),
    }

    impl TryFrom<Timestamp> for chrono::DateTime<Utc> {
        type Error = anyhow::Error;

        fn try_from(ts: Timestamp) -> Result<Self, Self::Error> {
            Ok(match ts {
                Timestamp::Epoch(ts) => match ts {
                    EpochTimestamp::Nano(nsecs) => chrono::DateTime::from_timestamp_nanos(nsecs),
                    EpochTimestamp::Micro(micros) => {
                        chrono::DateTime::from_timestamp_micros(micros)
                            .ok_or_else(|| anyhow!("invalid timestamp: {ts:?}"))?
                    }
                    EpochTimestamp::Milli(millis) => {
                        chrono::DateTime::from_timestamp_millis(millis)
                            .ok_or_else(|| anyhow!("invalid timestamp: {ts:?}"))?
                    }
                    EpochTimestamp::Second(secs) => chrono::DateTime::from_timestamp(secs, 0)
                        .ok_or_else(|| anyhow!("invalid timestamp: {ts:?}"))?,
                },
                Timestamp::DateTime(dt) => {
                    let datetime = chrono::NaiveDateTime::try_from(dt)?;
                    chrono::DateTime::from_naive_utc_and_offset(datetime, *Utc::now().offset())
                }
            })
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum EpochTimestamp {
        /// seconds since Jan 1, 1970
        Second(i64),
        /// milliseconds since Jan 1, 1970
        Milli(i64),
        /// microseconds since Jan 1, 1970
        Micro(i64),
        /// nanoseconds since Jan 1, 1970
        Nano(i64),
    }

    #[derive(Debug, Clone, PartialEq, Default)]
    pub struct DateTime {
        pub date: Date,
        pub date_sep: Option<String>,
        pub time: Option<Time>,
    }

    impl TryFrom<DateTime> for chrono::NaiveDateTime {
        type Error = anyhow::Error;

        fn try_from(dt: DateTime) -> Result<Self, Self::Error> {
            let date = chrono::NaiveDate::from_ymd_opt(dt.date.year, dt.date.month, dt.date.day)
                .ok_or_else(|| anyhow!("invalid datetime: {dt:?}"))?;
            let time = dt.time.map(chrono::NaiveTime::try_from).transpose()?;
            Ok(chrono::NaiveDateTime::new(date, time.unwrap_or_default()))
        }
    }

    #[derive(Debug, Clone, PartialEq, Default)]
    pub struct Date {
        pub year: i32,
        pub year_sep: Option<String>,
        pub month: u32,
        pub month_sep: Option<String>,
        pub day: u32,
    }

    impl TryFrom<Date> for chrono::NaiveDate {
        type Error = anyhow::Error;

        fn try_from(d: Date) -> Result<Self, Self::Error> {
            chrono::NaiveDate::from_ymd_opt(d.year, d.month, d.day)
                .ok_or_else(|| anyhow!("invalid date: {d:?}"))
        }
    }

    #[derive(Debug, Clone, PartialEq, Default)]
    pub struct Time {
        pub hour: u32,
        pub hour_sep: Option<String>,
        pub minute: u32,
        pub minute_sep: Option<String>,
        pub second: Option<u32>,
        pub second_sep: Option<String>,
        pub subsecond: Option<SubSecond>,
    }

    impl TryFrom<Time> for chrono::NaiveTime {
        type Error = anyhow::Error;

        fn try_from(t: Time) -> Result<Self, Self::Error> {
            let Time {
                hour,
                minute: min,
                second: sec,
                subsecond,
                ..
            } = t;
            let sec = sec.unwrap_or_default();
            match subsecond {
                Some(SubSecond::Milli(milli)) => {
                    chrono::NaiveTime::from_hms_milli_opt(hour, min, sec, milli)
                }
                Some(SubSecond::Micro(micro)) => {
                    chrono::NaiveTime::from_hms_micro_opt(hour, min, sec, micro)
                }
                Some(SubSecond::Nano(nano)) => {
                    chrono::NaiveTime::from_hms_nano_opt(hour, min, sec, nano)
                }
                None => chrono::NaiveTime::from_hms_opt(hour, min, sec),
            }
            .ok_or_else(|| anyhow!("invalid time: {hour:02?}:{min:02?}:{sec:02}"))
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum SubSecond {
        /// SSS
        Milli(u32),
        /// SSSSSS
        Micro(u32),
        /// SSSSSSSSS
        Nano(u32),
    }
}

mod resolver {
    use chrono::{Datelike, Timelike};

    use super::ast::{
        Date, DateTime, DoUndo, EpochTimestamp, PaddedNumber, PathTemplate, Segment, Semver,
        SubSecond, TemplateData, Time, Timestamp, Token, UpDown,
    };

    pub trait Resolve {
        fn resolve(&self, data: &TemplateData) -> String;
    }

    impl Resolve for PathTemplate {
        fn resolve(&self, data: &TemplateData) -> String {
            self.segments
                .iter()
                .map(|s| Resolve::resolve(s, data))
                .collect()
        }
    }

    impl Resolve for Segment {
        fn resolve(&self, data: &TemplateData) -> String {
            self.tokens
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    let next = self.tokens.get(i + 1);
                    // special case: when there's an UpDown token and we're not rendering it, also don't render the preceding Dot token.
                    if data.up_down.is_none()
                        && matches!(t, Token::Dot)
                        && matches!(next, Some(Token::UpDown(_)))
                    {
                        String::new()
                    } else {
                        Resolve::resolve(t, data)
                    }
                })
                .collect()
        }
    }

    impl Resolve for Token {
        fn resolve(&self, data: &TemplateData) -> String {
            match self {
                Token::Prefix(prefix) => prefix.clone(),
                Token::PaddedNumber(padding) => Resolve::resolve(padding, data),
                Token::RandomNumber(_) => {
                    if let Some(num) = data.random {
                        num.to_string()
                    } else {
                        data.timestamp.timestamp_micros().to_string()
                    }
                }
                Token::Semver(v) => Resolve::resolve(v, data),
                Token::Timestamp(ts) => Resolve::resolve(ts, data),
                Token::Name(_) => data.name.clone(),
                Token::UpDown(updown) => Resolve::resolve(updown, data),
                Token::DoUndo(updown) => Resolve::resolve(updown, data),
                Token::Underscore => "_".to_owned(),
                Token::Dot => ".".to_owned(),
                Token::Dash => "-".to_owned(),
                Token::Extension => ".sql".to_owned(),
            }
        }
    }

    impl Resolve for PaddedNumber {
        fn resolve(&self, data: &TemplateData) -> String {
            let counter = data.counter.unwrap_or(self.number + 1);
            format!("{:0>width$}", counter, width = self.width)
        }
    }

    impl Resolve for Semver {
        fn resolve(&self, data: &TemplateData) -> String {
            let num = if let Some(num) = data.semver.clone() {
                num
            } else {
                self.clone().increment_minor()
            };
            format!("{num}")
        }
    }

    impl Resolve for Timestamp {
        fn resolve(&self, data: &TemplateData) -> String {
            match self {
                Self::Epoch(ts) => Resolve::resolve(ts, data),
                Self::DateTime(dt) => Resolve::resolve(dt, data),
            }
        }
    }

    impl Resolve for EpochTimestamp {
        fn resolve(&self, data: &TemplateData) -> String {
            let ts = data.timestamp;
            match self {
                Self::Second(_) => ts.timestamp(),
                Self::Milli(_) => ts.timestamp_millis(),
                Self::Micro(_) => ts.timestamp_micros(),
                Self::Nano(_) => ts.timestamp_nanos_opt().unwrap_or(0),
            }
            .to_string()
        }
    }

    impl Resolve for DateTime {
        fn resolve(&self, data: &TemplateData) -> String {
            Resolve::resolve(&self.date, data)
                + self.date_sep.clone().unwrap_or_default().as_str()
                + self
                    .time
                    .as_ref()
                    .map(|t| Resolve::resolve(t, data))
                    .unwrap_or("".to_owned())
                    .as_str()
        }
    }

    impl Resolve for Date {
        fn resolve(&self, data: &TemplateData) -> String {
            let ts = data.timestamp;
            format!(
                "{:02}{}{:02}{}{:02}",
                ts.year(),
                self.year_sep.clone().unwrap_or_default(),
                ts.month(),
                self.month_sep.clone().unwrap_or_default(),
                ts.day()
            )
        }
    }

    impl Resolve for Time {
        fn resolve(&self, data: &TemplateData) -> String {
            let ts = data.timestamp;
            format!(
                "{:02}{}{:02}{}{:02}{}{}",
                ts.hour(),
                self.hour_sep.clone().unwrap_or_default(),
                ts.minute(),
                self.minute_sep.clone().unwrap_or_default(),
                self.second
                    .map(|_| format!("{:02}", ts.second()))
                    .unwrap_or_default(),
                self.second_sep.clone().unwrap_or_default(),
                self.subsecond
                    .as_ref()
                    .map(|sss| Resolve::resolve(sss, data))
                    .unwrap_or_default(),
            )
        }
    }

    impl Resolve for SubSecond {
        fn resolve(&self, data: &TemplateData) -> String {
            let ts = data.timestamp;
            match self {
                Self::Milli(_) => ts.timestamp_subsec_millis().to_string(),
                Self::Micro(_) => ts.timestamp_subsec_micros().to_string(),
                Self::Nano(_) => ts.timestamp_subsec_nanos().to_string(),
            }
        }
    }

    impl Resolve for UpDown {
        fn resolve(&self, data: &TemplateData) -> String {
            match data.up_down {
                Some(UpDown::Up) => "up",
                Some(UpDown::Down) => "down",
                None => "",
            }
            .to_owned()
        }
    }

    impl Resolve for DoUndo {
        fn resolve(&self, data: &TemplateData) -> String {
            match data.up_down {
                Some(UpDown::Up) => "do",
                Some(UpDown::Down) => "undo",
                None => "",
            }
            .to_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use chrono::Utc;

    use super::ast::{PathTemplate, Semver, TemplateData, Token, UpDown};

    fn data(tmpl: &PathTemplate) -> TemplateData {
        let mut data = TemplateData::default();
        let mut timestamp = data.timestamp;
        tmpl.segments
            .iter()
            .flat_map(|s| &s.tokens)
            .for_each(|t| {
                match t {
                    Token::Timestamp(ts) => timestamp = ts.clone().try_into().unwrap(),
                    Token::Name(name) => data.name = name.clone(),
                    Token::PaddedNumber(padding) => data.counter = Some(padding.number),
                    Token::RandomNumber(rand) => data.random = Some(*rand),
                    Token::Semver(semver) => data.semver = Some(semver.clone()),
                    Token::UpDown(updown) => {
                        data.up_down = Some(updown.clone());
                    }
                    Token::DoUndo(doundo) => {
                        data.up_down = Some(doundo.clone().into());
                    }
                    // the rest of the data is used directly
                    _ => {}
                };
            });
        data.timestamp = timestamp;
        data
    }

    #[test]
    fn test_parse_resolve() {
        vec![
            "1741141452_generated_migration.down.sql",
            "000522_add_users_full_name.undo.sql",
            "000522_create_users.do.sql",
            "000522_inital_schema.sql",
            "002_create_users_table.sql",
            "006_create_categories_table.sql",
            "010_add_foreign_key_to_posts.sql",
            "014_add_roles_to_users.sql",
            "017_create_logs_table.sql",
            "020_add_soft_delete_to_users.sql",
            "1007728000000000000_inital_schema.sql",
            "1007728000000000_inital_schema.sql",
            "1007728000000_inital_schema.sql",
            "1007728000_inital_schema.sql",
            "1036400000000000000_create_users.do.sql",
            "1036400000000000_create_users.sql",
            "1036400000000_create_users.sql",
            "1065072000000000000_add_users_full_name.undo.sql",
            "1704067200123_add_users_full_name.sql",
            "1704067200_add_users_full_name.sql",
            "1798675200123456_add_users_full_name.sql",
            "1893283200_create_users.sql",
            "2001-12-07.07-26-400_inital_schema.sql",
            "2002-11-04.03-53-200_create_users.up.sql",
            "2003-10-02.01-20-000_add_users_full_name.down.sql",
            "2023-01-04_add_comments_table.sql",
            "2023-01-12_add_tags_to_posts.sql",
            "2023-01-18_add_timestamp_to_posts.sql",
            "20230101_initial_setup.sql",
            "20230108_drop_comments_table.sql",
            "20230115_create_settings_table.sql",
            "v1_create_posts_table.sql",
            "v200112070726400_inital_schema.sql",
            "v200211040353200_create_users.up.sql",
            "v200211040353200_create_users.up.sql",
            "v20201231190000123456_add_users_full_name.down.sql",
            "v2_create_tags_table.sql",
            "v2.2.2_create_tags_table.sql",
            "v11.12.13_create_tags_table.sql",
            "v88.99.00_create_tags_table.sql",
            "11.12.13_create_tags_table.sql",
            "0011.0012.0013_create_tags_table.sql",
            "zv2234234203984209384_oops_we_ran_out_of_digits.sql",
            // dirs
            "017_create_logs_table/do.sql",
            "1704067200_add_users_full_name/up.sql",
            "2003-10-02.01-20-000_add_users_full_name/down.sql",
            "v1_create_posts_table/up.sql",
            "v20201231190000123456_add_users_full_name/down.sql",
            "v0.1.0_add_users_full_name/down.sql",
            "v11.12.13_add_users_full_name/down.sql",
            "11.12.13_add_users_full_name/down.sql",
            "1011.0012.0013_add_users_full_name/down.sql",
        ]
        .into_iter()
        .enumerate()
        .for_each(|(i, input)| {
            eprintln!("{input:?}");
            let template = super::parser::parse(input)
                .context(format!("test case {i:02}"))
                .unwrap_or_else(|_| panic!("{input} should parse"));
            let data = data(&template);
            let template = template.with_up_down();
            let out = template.resolve(&data);
            assert_eq!(
                out, input,
                "template should resolve to input\n{template:?}\n{data:?}"
            );

            vec![
                |data: TemplateData| TemplateData {
                    name: "some_other_name".to_owned(),
                    ..data
                },
                |data: TemplateData| TemplateData {
                    timestamp: Utc::now(),
                    counter: Some(data.counter.map_or(1, |c| c + 1)),
                    random: Some(data.random.map_or(1, |r| r + 1)),
                    semver: Some(
                        data.semver
                            .map_or(Semver::default(), |s| s.increment_minor()),
                    ),
                    ..data
                },
                |data: TemplateData| TemplateData {
                    up_down: match data.up_down {
                        Some(UpDown::Up) => Some(UpDown::Down),
                        Some(UpDown::Down) => Some(UpDown::Up),
                        None => Some(UpDown::Up),
                    },
                    ..data
                },
            ]
            .into_iter()
            .for_each(|f| {
                let data = f(data.clone());
                let out = template.resolve(&data);
                assert_ne!(
                    out, input,
                    "template should adapt based on input data\n{template:?}\n{data:?}"
                );
            });
        });
    }
}
