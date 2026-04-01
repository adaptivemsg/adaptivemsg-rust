use crate::message::Message;

pub fn expected_wire_name<T: Message>() -> &'static str {
    T::wire_name_static()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::OkReply;
    use crate::message::ErrorReply;

    #[test]
    fn expected_wire_name_ok_reply() {
        let name = expected_wire_name::<OkReply>();
        assert!(!name.is_empty());
        assert!(name.contains("OkReply"));
    }

    #[test]
    fn expected_wire_name_error_reply() {
        let name = expected_wire_name::<ErrorReply>();
        assert!(!name.is_empty());
        assert!(name.contains("ErrorReply"));
    }
}
