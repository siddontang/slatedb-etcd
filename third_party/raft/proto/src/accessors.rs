// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// The Raft core uses these field accessors. Prost generates all wire types and enum setters.
use super::eraftpb::*;
use prost::bytes::Bytes;

macro_rules! field {
    ($field:ident, $ty:ty, $get:ident, $set:ident) => {
        pub fn $get(&self) -> $ty {
            self.$field
        }
        pub fn $set(&mut self, value: $ty) {
            self.$field = value;
        }
    };
}

macro_rules! enumeration {
    ($field:ident, $ty:ty, $get:ident) => {
        pub fn $get(&self) -> $ty {
            self.$field()
        }
    };
}

macro_rules! buffer {
    ($field:ident, $get:ident, $set:ident, $take:ident) => {
        pub fn $get(&self) -> &[u8] {
            &self.$field
        }
        pub fn $set(&mut self, value: Bytes) {
            self.$field = value;
        }
        pub fn $take(&mut self) -> Bytes {
            std::mem::take(&mut self.$field)
        }
    };
}

macro_rules! repeated {
    ($field:ident, $ty:ty, $get:ident, $set:ident, $edit:ident, $take:ident) => {
        pub fn $get(&self) -> &[$ty] {
            &self.$field
        }
        pub fn $set(&mut self, value: Vec<$ty>) {
            self.$field = value;
        }
        pub fn $edit(&mut self) -> &mut Vec<$ty> {
            &mut self.$field
        }
        pub fn $take(&mut self) -> Vec<$ty> {
            std::mem::take(&mut self.$field)
        }
    };
}

macro_rules! nested {
    ($field:ident, $ty:ty, $get:ident, $set:ident, $edit:ident, $take:ident) => {
        pub fn $get(&self) -> &$ty {
            static EMPTY: std::sync::OnceLock<$ty> = std::sync::OnceLock::new();
            self.$field
                .as_ref()
                .unwrap_or_else(|| EMPTY.get_or_init(Default::default))
        }
        pub fn $set(&mut self, value: $ty) {
            self.$field = Some(value);
        }
        pub fn $edit(&mut self) -> &mut $ty {
            self.$field.get_or_insert_with(Default::default)
        }
        pub fn $take(&mut self) -> $ty {
            self.$field.take().unwrap_or_default()
        }
    };
}

impl Entry {
    enumeration!(entry_type, EntryType, get_entry_type);
    buffer!(data, get_data, set_data, take_data);
    buffer!(context, get_context, set_context, take_context);
    field!(index, u64, get_index, set_index);
    field!(term, u64, get_term, set_term);
}

impl Message {
    enumeration!(msg_type, MessageType, get_msg_type);
    repeated!(
        entries,
        Entry,
        get_entries,
        set_entries,
        mut_entries,
        take_entries
    );
    nested!(
        snapshot,
        Snapshot,
        get_snapshot,
        set_snapshot,
        mut_snapshot,
        take_snapshot
    );
    buffer!(context, get_context, set_context, take_context);
    field!(index, u64, get_index, set_index);
    field!(commit, u64, get_commit, set_commit);
}

impl Snapshot {
    buffer!(data, get_data, set_data, take_data);
    nested!(
        metadata,
        SnapshotMetadata,
        get_metadata,
        set_metadata,
        mut_metadata,
        take_metadata
    );
}

impl SnapshotMetadata {
    nested!(
        conf_state,
        ConfState,
        get_conf_state,
        set_conf_state,
        mut_conf_state,
        take_conf_state
    );
    field!(index, u64, get_index, set_index);
    field!(term, u64, get_term, set_term);
}

impl ConfState {
    repeated!(voters, u64, get_voters, set_voters, mut_voters, take_voters);
    repeated!(
        learners,
        u64,
        get_learners,
        set_learners,
        mut_learners,
        take_learners
    );
    repeated!(
        voters_outgoing,
        u64,
        get_voters_outgoing,
        set_voters_outgoing,
        mut_voters_outgoing,
        take_voters_outgoing
    );
    repeated!(
        learners_next,
        u64,
        get_learners_next,
        set_learners_next,
        mut_learners_next,
        take_learners_next
    );
}

impl ConfChange {
    enumeration!(change_type, ConfChangeType, get_change_type);
    buffer!(context, get_context, set_context, take_context);
}

impl ConfChangeSingle {
    enumeration!(change_type, ConfChangeType, get_change_type);
}

impl ConfChangeV2 {
    enumeration!(transition, ConfChangeTransition, get_transition);
    repeated!(
        changes,
        ConfChangeSingle,
        get_changes,
        set_changes,
        mut_changes,
        take_changes
    );
    buffer!(context, get_context, set_context, take_context);
}
