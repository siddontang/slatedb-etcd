// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// We use `default` method a lot to be support prost and rust-protobuf at the
// same time. And reassignment can be optimized by compiler.
#![allow(clippy::field_reassign_with_default)]
#![allow(mismatched_lifetime_syntaxes)]
#![allow(unused_parens)]

mod confchange;
mod confstate;

pub use crate::confchange::{
    new_conf_change_single, parse_conf_change, stringify_conf_change, ConfChangeI,
};
pub use crate::confstate::conf_state_eq;
pub mod eraftpb {
    include!(concat!(env!("OUT_DIR"), "/eraftpb.rs"));
}

mod accessors;

/// These methods connect the upstream Raft core to Prost.
pub mod protocompat {
    pub use prost as pbcrate;
    pub use prost::{DecodeError as PbError, Message as PbMessage};

    pub trait PbMessageExt: PbMessage + Sized {
        fn compute_size(&self) -> u32 {
            self.encoded_len() as u32
        }

        fn merge_from_bytes(&mut self, bytes: &[u8]) -> Result<(), PbError> {
            self.merge(bytes)
        }

        fn write_to_bytes(&self) -> Result<Vec<u8>, PbError> {
            Ok(self.encode_to_vec())
        }
    }

    impl<T: PbMessage> PbMessageExt for T {}

    pub trait ProtobufEnum {}
}

impl eraftpb::Snapshot {
    pub fn is_empty(&self) -> bool {
        self.get_metadata().index == 0
    }
}

pub mod prelude {
    pub use crate::eraftpb::{
        ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
        ConfState, Entry, EntryType, HardState, Message, MessageType, Snapshot, SnapshotMetadata,
    };
}

pub mod util {
    use crate::eraftpb::ConfState;

    impl<Iter1, Iter2> From<(Iter1, Iter2)> for ConfState
    where
        Iter1: IntoIterator<Item = u64>,
        Iter2: IntoIterator<Item = u64>,
    {
        fn from((voters, learners): (Iter1, Iter2)) -> Self {
            let mut conf_state = ConfState::default();
            conf_state.mut_voters().extend(voters);
            conf_state.mut_learners().extend(learners);
            conf_state
        }
    }
}
