use super::*;

#[test]
fn it_parses_server_command_connected() {
    let input = "CONNECTED\r\n";
    parse_server_command(input.as_bytes()).expect("it parsed server command");
}

#[test]
fn it_parses_header_key() {
    let input = "accept-version:\n";
    let (_remaining, header) = parse_header(input.as_bytes()).expect("it parsed header");
    assert_eq!(header.key, "accept-version");
    assert_eq!(header.value, None);
}

#[test]
fn it_parses_header_key_and_value() {
    let input = "accept-version:whatever\n";
    let (_remaining, header) = parse_header(input.as_bytes()).expect("it parsed header");
    assert_eq!(header.key, "accept-version");
    assert_eq!(header.value, Some("whatever".to_string()));
}

#[test]
fn it_parses_connect_frame_trailing_newlines() {
    let input = "CONNECT\r\naccept-version:1.2\nhost:ahost\n\n\x00\n\n\n\n\n\r\n";
    let (remaining, _frame) = parse_frame(input.as_bytes()).expect("it parsed frame");
    assert_eq!(remaining, [] as [u8; 0]);
}

#[test]
fn it_parses_connect_frame_no_trail() {
    let input = "CONNECT\r\naccept-version:1.2\nhost:ahost\n\n\x00";
    let (remaining, _frame) = parse_frame(input.as_bytes()).expect("it parsed frame");
    assert_eq!(remaining, [] as [u8; 0]);
}

#[test]
fn it_parses_connect_frame_content_length_zero() {
    let input = "CONNECT\r\naccept-version:1.2\nhost:ahost\ncontent-length:0\n\n\x00";
    let (remaining, _frame) = parse_frame(input.as_bytes()).expect("it parsed frame");
    assert_eq!(remaining, [] as [u8; 0]);
}

#[test]
fn it_parses_send_frame_no_content_length() {
    let input = "SEND\r\ndestination:somewhere\n\nab\x00";
    let (remaining, _frame) = parse_frame(input.as_bytes()).expect("it parsed frame");
    assert_eq!(remaining, [] as [u8; 0]);
}

#[test]
fn it_parses_send_frame_content_length() {
    let input = "SEND\r\ndestination:somewhere\ncontent-length:2\n\nab\x00";
    let (remaining, _frame) = parse_frame(input.as_bytes()).expect("it parsed frame");
    assert_eq!(remaining, [] as [u8; 0]);
}

#[test]
fn it_parses_send_frame_content_length_null_body() {
    let input = "SEND\r\ndestination:somewhere\ncontent-length:2\n\n\x00\x00\x00";
    let (remaining, _frame) = parse_frame(input.as_bytes()).expect("it parsed frame");
    assert_eq!(remaining, [] as [u8; 0]);
}

#[test]
fn it_parses_send_frame_no_content_length_null_body() {
    let input = "SEND\r\ndestination:somewhere\n\n\x00\x00\x00";
    let (remaining, _frame) = parse_frame(input.as_bytes()).expect("it parsed frame");
    assert_eq!(remaining, [0, 0]);
}
