pub mod mvccpb {
    tonic::include_proto!("mvccpb");
}

pub mod etcdserverpb {
    tonic::include_proto!("etcdserverpb");
}

pub mod authpb {
    tonic::include_proto!("authpb");
}

pub(crate) mod peer {
    tonic::include_proto!("slatedb.peer");
}

pub(crate) mod command {
    tonic::include_proto!("slatedb_command");
}

pub(crate) mod state {
    tonic::include_proto!("slatedb_state");
}
