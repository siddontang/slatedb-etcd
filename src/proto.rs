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

/// The gRPC codec of every service: prost, with the encode buffer sized before encoding.
pub(crate) mod codec {
    use prost::Message;
    use tonic::codec::{EncodeBuf, Encoder};
    use tonic::Status;
    use tonic_prost::{ProstCodec, ProstDecoder, ProstEncoder};

    /// `tonic_prost::ProstCodec` with an encoder that reserves the encoded length first.
    ///
    /// prost writes a message one field at a time, so the encode buffer grows by doubling. For an
    /// unpaginated Range of 1.2 GiB, each doubling copied the encoded prefix and faulted in new
    /// pages. That took about one fifth of the server CPU of the Range. The wire bytes do not
    /// change.
    #[derive(Debug, Clone)]
    pub(crate) struct Codec<T, U>(ProstCodec<T, U>);

    impl<T, U> Default for Codec<T, U> {
        fn default() -> Self {
            Self(ProstCodec::default())
        }
    }

    impl<T, U> tonic::codec::Codec for Codec<T, U>
    where
        T: Message + Send + 'static,
        U: Message + Default + Send + 'static,
    {
        type Encode = T;
        type Decode = U;
        type Encoder = SizedEncoder<T>;
        type Decoder = ProstDecoder<U>;

        fn encoder(&mut self) -> Self::Encoder {
            SizedEncoder(self.0.encoder())
        }

        fn decoder(&mut self) -> Self::Decoder {
            self.0.decoder()
        }
    }

    #[derive(Debug)]
    pub(crate) struct SizedEncoder<T>(ProstEncoder<T>);

    impl<T: Message> Encoder for SizedEncoder<T> {
        type Item = T;
        type Error = Status;

        fn encode(&mut self, item: T, buf: &mut EncodeBuf<'_>) -> Result<(), Status> {
            buf.reserve(item.encoded_len());
            self.0.encode(item, buf)
        }

        fn buffer_settings(&self) -> tonic::codec::BufferSettings {
            self.0.buffer_settings()
        }
    }
}
