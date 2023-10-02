#[async_trait]
pub trait KV: Clone + Display + Send + Sync {
    /// Read returns the value for a given key in the key/value store.
    /// Returns an RPCError error with a KeyDoesNotExist code if the key does not exist.
    async fn read<T>(&self, key: String) -> Result<T>
    where
        T: Deserialize<'static> + Send;

    /// Write overwrites the value for a given key in the key/value store.
    async fn write<T>(&self, key: String, val: T) -> Result<()>
    where
        T: Serialize + Send;

    /// CAS updates the value for a key if its current value matches the
    /// previous value. Creates the key if it is not exist is requested.
    ///
    /// Returns an RPCError with a code of PreconditionFailed if the previous value
    /// does not match. Return a code of KeyDoesNotExist if the key did not exist.
    async fn cas<T>(&self, ctx: Context, key: String, from: T, to: T, put: bool) -> Result<()>
    where
        T: Serialize + Deserialize<'static> + Send;
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload<T> {
    /// KVReadMessageBody represents the body for the KV "read" message.
    Read {
        key: String,
    },
    /// KVReadOKMessageBody represents the response body for the KV "read_ok" message.
    ReadOk {
        value: T,
    },
    /// KVWriteMessageBody represents the body for the KV "cas" message.
    Write {
        key: String,
        value: T,
    },
    /// KVCASMessageBody represents the body for the KV "cas" message.
    Cas {
        key: String,
        from: T,
        to: T,
        #[serde(
            default,
            rename = "create_if_not_exists",
            skip_serializing_if = "is_ref_false"
        )]
        put: bool,
    },
    CasOk {},
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_ref_false(b: &bool) -> bool {
    !*b
}
