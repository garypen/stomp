use std::str::FromStr;

use super::*;

#[test]
fn it_finds_header_keys() {
    let frame = Frame::connected::<String>(None);
    assert!(frame.has_keys(&["version"]));
    let frame = Frame::stomp("ahost", vec![]);
    assert!(frame.has_keys(&["host", "accept-version"]));
}

#[test]
fn it_serializes_client_individual_enum() {
    assert_eq!(
        SubscriptionMode::Individual,
        SubscriptionMode::from_str("client-individual").expect("it converts to enum")
    );
    assert_eq!(
        "client-individual".to_string(),
        SubscriptionMode::Individual.to_string()
    );
}
