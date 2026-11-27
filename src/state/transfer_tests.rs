use super::*;

/// A record above its checkpoint row carries the row after the encoded fields. An earlier
/// binary decodes the same fields and ignores the row. A record without a row has none.
#[test]
fn a_view_record_carries_its_row_after_the_earlier_fields() {
    #[derive(serde::Deserialize)]
    struct Earlier {
        version: u32,
        checkpoint: String,
    }
    let id = uuid::Uuid::new_v4();
    let above = ViewDescriptor::above_row(id, 7).encode().unwrap();
    let decoded = ViewDescriptor::decode(&above).unwrap().unwrap();
    assert_eq!((decoded.checkpoint().unwrap(), decoded.row), (id, Some(7)));
    let earlier: Earlier = crate::codec::decode(&above).unwrap();
    assert_eq!(
        (earlier.version, earlier.checkpoint),
        (VIEW_SNAPSHOT_FORMAT, id.to_string())
    );
    let exact = ViewDescriptor::new(id).encode().unwrap();
    assert_eq!(ViewDescriptor::decode(&exact).unwrap().unwrap().row, None);
    assert!(exact.len() < above.len());
}
