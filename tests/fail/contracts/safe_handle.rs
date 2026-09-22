use ckb_rocksdb::Handle;
struct Forged;
impl Handle<u8> for Forged {
    fn handle(&self) -> *mut u8 { std::ptr::null_mut() }
}
fn main() {}
