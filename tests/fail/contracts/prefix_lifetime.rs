use ckb_rocksdb::SliceTransform;
fn retain_forever(input: &'static [u8]) -> &'static [u8] { input }
fn main() {
    let _transform = SliceTransform::create("invalid", retain_forever, None);
}
