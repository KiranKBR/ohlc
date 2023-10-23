use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::{Arc, RwLock};
use ohlc::ohlc::{OHLCMaker, make_batch_ohlc};
use ohlc::tools::datas::{TickData};
use ohlc::tools::tick_generator::{TickGenerator};


// this one for multi-threaded 
fn criterion_benchmark_parallel(c: &mut Criterion) {
    c.bench_function("benchmark", |b| {
        b.iter(|| {
            let maker = OHLCMaker::new();
            let tick_generator = TickGenerator::new();
            let size = 1000000;
            let tick_datas = Arc::new(RwLock::new(tick_generator.from_mock(size)));
            let ohlc_datas = maker.make_ohlc_parallel(tick_datas, 10);
            assert_eq!(ohlc_datas.len(), size);
        })
    });
}

// this one for single threaded 
fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("benchmark", |b| b.iter(|| {
        let tick_generator = TickGenerator::new();
        let size = 1000000;
        let tick_datas = Arc::new(tick_generator.from_mock(size));
        let local_data: &Vec<TickData> = &*tick_datas;
        let ohlc_datas = make_batch_ohlc(local_data, 10, 0, size - 1, 0);
        assert_eq!(ohlc_datas.len(), size);
    }));
}
// multi-threaed one is better than single threaded

criterion_group!(
    name = benches; 
    config = Criterion::default().sample_size(20);
    targets = criterion_benchmark_parallel);
criterion_main!(benches);