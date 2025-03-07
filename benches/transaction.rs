use ckb_rocksdb::{OptimisticTransactionDB, TemporaryDBPath, prelude::*};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};

const BIG_VALUE: [u8; 1024] = [0u8; 1024];
const NUM: u64 = 10000;

pub fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction");

    group.bench_with_input(BenchmarkId::new("transaction Get", NUM), &NUM, |b, size| {
        b.iter_batched(
            || {
                let path = TemporaryDBPath::new();
                let mut opts = Options::default();
                opts.create_if_missing(true);

                let db = OptimisticTransactionDB::open_default(&path).unwrap();

                for i in 0..*size {
                    db.put(&i.to_le_bytes()[..], &BIG_VALUE[..]).unwrap();
                }
                let trans = db.transaction_default();
                for i in *size..(size * 2) {
                    trans.put(&i.to_le_bytes()[..], &BIG_VALUE[..]).unwrap();
                }

                (trans, db, path)
            },
            |(trans, _db, _path)| {
                for i in 0..(size * 2) {
                    trans.get(&i.to_le_bytes()[..]).unwrap().unwrap();
                }
            },
            BatchSize::PerIteration,
        )
    });

    group.bench_with_input(
        BenchmarkId::new("transaction GetPinned", NUM),
        &NUM,
        |b, size| {
            b.iter_batched(
                || {
                    let path = TemporaryDBPath::new();
                    let mut opts = Options::default();
                    opts.create_if_missing(true);

                    let db = OptimisticTransactionDB::open_default(&path).unwrap();

                    for i in 0..*size {
                        db.put(&i.to_le_bytes()[..], &BIG_VALUE[..]).unwrap();
                    }
                    let trans = db.transaction_default();
                    for i in *size..(size * 2) {
                        trans.put(&i.to_le_bytes()[..], &BIG_VALUE[..]).unwrap();
                    }

                    (trans, db, path)
                },
                |(trans, _db, _path)| {
                    for i in 0..(size * 2) {
                        trans.get_pinned(&i.to_le_bytes()[..]).unwrap().unwrap();
                    }
                },
                BatchSize::PerIteration,
            )
        },
    );
}

criterion_group!(benches, bench);
criterion_main!(benches);
