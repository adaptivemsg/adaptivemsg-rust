use crate::message::Message;

pub fn expected_wire_name<T: Message>() -> &'static str {
    T::wire_name_static()
}
