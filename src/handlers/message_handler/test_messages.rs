use anyhow::Result;
use bytes::{BufMut, BytesMut};

use super::messages::{Message, Ping, SerializedMessage};

#[test]
fn test_serialize_and_parse() -> Result<()> {
    tracing_subscriber::fmt::init();

    let msg = Message::new(Box::new(Ping::Ping));

    let ser_msg = SerializedMessage::new(&msg)?;
    let msg_data = msg.to_bytes()?;

    let mut buf = BytesMut::new();
    buf.put(&msg_data[..]);

    println!("msg_data.len(): {}", msg_data.len());
    println!("ser_msg: {:?}", ser_msg);
    println!("msg_data: {:?}", msg_data);

    let parsed_ser_msg = SerializedMessage::parse(&mut buf)?;

    println!("parsed_ser_msg: {:?}", parsed_ser_msg);

    assert_eq!(ser_msg, parsed_ser_msg);

    Ok(())
}
