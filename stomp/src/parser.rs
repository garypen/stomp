use std::str::FromStr;

use crate::ClientCommand;
use crate::Command;
use crate::Frame;
use crate::Header;
use crate::ServerCommand;

use nom::branch::alt;
use nom::bytes::streaming::tag;
use nom::bytes::streaming::take;
use nom::bytes::streaming::take_till;
use nom::bytes::streaming::take_while;
use nom::bytes::streaming::take_while1;
use nom::character::streaming::line_ending;
use nom::combinator::complete;
use nom::combinator::eof;
use nom::combinator::opt;
use nom::error::Error;
use nom::error::ErrorKind;
use nom::multi::many0;
use nom::multi::many_till;
use nom::sequence::separated_pair;
use nom::sequence::terminated;
use nom::IResult;

#[cfg(test)]
mod test;

type EolResult<'a> = IResult<&'a [u8], (Vec<&'a [u8]>, &'a [u8])>;

fn parse_client_command(input: &[u8]) -> IResult<&[u8], ClientCommand> {
    let (remaining, s_cmd) =
        terminated(take_while1(|c: u8| c.is_ascii_alphabetic()), line_ending)(input)?;
    let cmd = ClientCommand::from_str(
        std::str::from_utf8(s_cmd)
            .map_err(|_e| nom::Err::Failure(Error::new(s_cmd, ErrorKind::Fail)))?,
    )
    .map_err(|_e| nom::Err::Failure(Error::new(s_cmd, ErrorKind::Fail)))?;
    Ok((remaining, cmd))
}

fn parse_server_command(input: &[u8]) -> IResult<&[u8], ServerCommand> {
    let (remaining, s_cmd) =
        terminated(take_while1(|c: u8| c.is_ascii_alphabetic()), line_ending)(input)?;
    let cmd = ServerCommand::from_str(
        std::str::from_utf8(s_cmd)
            .map_err(|_e| nom::Err::Failure(Error::new(s_cmd, ErrorKind::Fail)))?,
    )
    .map_err(|_e| nom::Err::Failure(Error::new(s_cmd, ErrorKind::Fail)))?;
    Ok((remaining, cmd))
}

fn parse_command(input: &[u8]) -> IResult<&[u8], Command> {
    // Try to parse a server command first
    let (remaining, cmd) = parse_server_command(input)
        .map(|(remaining, sc)| (remaining, Command::ServerCommand(sc)))
        .or_else(|_| {
            parse_client_command(input)
                .map(|(remaining, cc)| (remaining, Command::ClientCommand(cc)))
        })?;
    Ok((remaining, cmd))
}

fn parse_header_key(input: &[u8]) -> IResult<&[u8], String> {
    let (remaining, b_key) = take_while1(|c: u8| c != b':' && c != b'\r' && c != b'\n')(input)?;
    let key = std::str::from_utf8(b_key)
        .map_err(|_e| nom::Err::Failure(Error::new(b_key, ErrorKind::Fail)))?;
    Ok((remaining, key.to_string()))
}

fn parse_header_value(input: &[u8]) -> IResult<&[u8], String> {
    let (remaining, b_value) = take_while(|c: u8| c != b':' && c != b'\r' && c != b'\n')(input)?;

    if b_value.is_empty() {
        return Err(nom::Err::Error(Error::new(b_value, ErrorKind::Fail)));
    }
    let value = std::str::from_utf8(b_value)
        .map_err(|_e| nom::Err::Failure(Error::new(b_value, ErrorKind::Fail)))?;
    Ok((remaining, value.to_string()))
}

fn parse_header(input: &[u8]) -> IResult<&[u8], Header> {
    let (remaining, (key, value)) = terminated(
        separated_pair(parse_header_key, tag(":"), opt(parse_header_value)),
        line_ending,
    )(input)?;
    let header = Header { key, value };
    Ok((remaining, header))
}

fn parse_headers(input: &[u8]) -> IResult<&[u8], Vec<Header>> {
    let (remaining, headers) = many0(parse_header)(input)?;
    Ok((remaining, headers))
}

fn parse_body(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (remaining, b_value) = take_while(|c: u8| c != b'\x00')(input)?;
    Ok((remaining, b_value))
}

fn parse_eols(input: &[u8]) -> EolResult<'_> {
    many_till(
        line_ending,
        alt((eof, complete(take_till(|c| c != b'\r' && c != b'\n')))),
    )(input)
}

pub fn parse_frame(input: &[u8]) -> IResult<&[u8], Frame> {
    // TODO: Think about if this is correct. We may have EOLs hanging around from our last frame,
    // so try to clear them out before we start to parse command. However, that means our
    // interpretation of the spec is a bit loose. It would be better to get rid of the EOLs at
    // just the end of this function.
    let (remaining, _) = parse_eols(input)?;
    let (remaining, command) = parse_command(remaining)?;
    let (remaining, headers) = terminated(parse_headers, line_ending)(remaining)?;
    // If our headers have a content-length, just read that many bytes
    let (remaining, body) = if let Some(length) = headers
        .iter()
        .find(|header| header.key == "content-length")
        .and_then(|header| header.value.clone())
    {
        let len = length
            .parse::<usize>()
            .map_err(|_e| nom::Err::Failure(Error::new(remaining, ErrorKind::Fail)))?;
        terminated(take(len), tag(b"\x00"))(remaining)?
    } else {
        terminated(parse_body, tag(b"\x00"))(remaining)?
    };
    let (remaining, _) = parse_eols(remaining)?;
    let frame = Frame::new(command, headers, body.to_vec());
    if frame.valid() {
        Ok((remaining, frame))
    } else {
        Err(nom::Err::Failure(nom::error::Error::new(
            remaining,
            nom::error::ErrorKind::Fail,
        )))
    }
}
