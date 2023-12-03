use std::path::{Path, PathBuf};

use polars::prelude::*;

#[derive(Debug)]
enum Error {
    IO(std::io::Error),
    Polars(PolarsError),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::IO(value)
    }
}

impl From<PolarsError> for Error {
    fn from(value: PolarsError) -> Self {
        Error::Polars(value)
    }
}

fn csv_to_parquet(
    csv_path: impl Into<PathBuf>,
    parquet_path: impl AsRef<Path>,
) -> Result<(), Error> {
    let mut df = CsvReader::from_path(csv_path)?.finish()?;
    let mut file = std::fs::File::create(parquet_path)?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}

fn parquet_to_csv(parquet_path: impl AsRef<Path>, csv_path: impl AsRef<Path>) -> Result<(), Error> {
    let mut file = std::fs::File::open(parquet_path)?;
    let mut df = ParquetReader::new(&mut file).finish()?;
    let mut file = std::fs::File::create(csv_path)?;
    CsvWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}

fn main() {
    let df1 = CsvReader::from_path(
        "/home/alex/data/binance-data/BTCUSDT/BTCUSDT-bookTicker-2023-11-01.csv",
    )
    .unwrap()
    .finish()
    .unwrap();

    let df2 = CsvReader::from_path(
        "/home/alex/data/binance-data/BTCUSDT/BTCUSDT-bookTicker-2023-11-01--from-parquet.csv",
    )
    .unwrap()
    .finish()
    .unwrap();

    if df1 == df2 {
        println!("dataframes are equal!")
    } else {
        println!("dataframes are not equal!")
    }

    println!("{}", df1.height());
    println!("{}", df2.height());

    // let df = LazyCsvReader::new(
    //     "/home/alex/data/binance-data/BTCUSDT/BTCUSDT-bookTicker-2023-11-01.csv",
    // )
    // .finish()
    // .unwrap();

    // let mut file = std::fs::File::create(
    //     "/home/alex/data/binance-data/BTCUSDT/BTCUSDT-bookTicker-2023-11-01.parquet",
    // )
    // .unwrap();

    // ParquetWriter::new(&mut file)
    //     .finish(&mut df.collect().unwrap())
    //     .unwrap();

    // parquet_to_csv(
    //     "/home/alex/data/binance-data/BTCUSDT/BTCUSDT-bookTicker-2023-11-01.parquet",
    //     "/home/alex/data/binance-data/BTCUSDT/BTCUSDT-bookTicker-2023-11-01--from-parquet.csv",
    // )
    // .unwrap();
}
