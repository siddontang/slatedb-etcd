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
