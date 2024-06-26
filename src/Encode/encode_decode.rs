use std::fs::File;
use std::sync::Arc;
use parquet::arrow::ArrowWriter;
use arrow::array::{Int32Array, ArrayRef, PrimitiveArray, Array};

use arrow::array::BinaryArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use bytes::Bytes;



// fn main() {
//         let key = vec![Bytes::from("ball"), Bytes::from("bat"), Bytes::from("glove"), Bytes::from("glove")];
//         let value = vec![Bytes::from("ayo huh"), Bytes::from("ayo 121huh"), Bytes::from("ayuh"), Bytes::from("ayh")];
//         write_parquet(key, value);
//         let res = read_parquet();
//         println!("Key: {:?}", res.0);
//         println!("Value: {:?}", res.1);
//
// }


fn write_parquet(filename:&str, key: Vec<Bytes>, value: Vec<Bytes>){
        let file = File::create(filename).unwrap();

        let key: Vec<&[u8]> = key.iter().map(|b| b.as_ref()).collect();
        let vals: Vec<&[u8]> = value.iter().map(|b| b.as_ref()).collect();
        let ids = BinaryArray::from(key);
        let vals = BinaryArray::from(vals);


        let fields = vec![
                Field::new("id", DataType::Binary, false),
                Field::new("val", DataType::Binary, false),
        ];
        let schema = Schema::new(fields);

        let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                        Arc::new(ids) as ArrayRef,
                        Arc::new(vals) as ArrayRef,
                ],
        ).unwrap();



        // WriterProperties can be used to set Parquet file options
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        // println!("Schema is: {:?}", batch.schema());
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).expect("Writing batch");
        // writer must be closed to write footer
        writer.close().unwrap();
}
fn arr_to_vec(binary_array: &BinaryArray) -> Vec<Bytes> {
        let mut ret = Vec::new();
        for i in 0..binary_array.len() {
                let value = binary_array.value(i);
                let value_bytes = Bytes::copy_from_slice(value);
                ret.push(value_bytes);
        }
        return ret;
}
fn read_parquet(filename: &str)-> (Vec<Bytes>, Vec<Bytes>){
        //Will explode if you have more than one collumn

        let file = File::open(filename).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        // println!("Converted arrow schema is: {}", builder.schema());
        let mut reader = builder.build().unwrap();
        let record_batch = reader.next().unwrap().unwrap();
        // println!("Read {} records.", record_batch.num_rows());
        // Print out the content of the Parquet file
        let key_arr_ref = record_batch.column(0);
        let value_arr_ref = record_batch.column(1);

        let ret_key = arr_to_vec(key_arr_ref.as_any().downcast_ref::<BinaryArray>().unwrap());
        let ret_value = arr_to_vec(value_arr_ref.as_any().downcast_ref::<BinaryArray>().unwrap());



        return (ret_key, ret_value) ;


}

fn combine_parquets(input_files: Vec<&str>, output_file: &str) -> (){
        let file = File::create(output_file).unwrap();
        let fields = vec![
                Field::new("id", DataType::Binary, false),
                Field::new("val", DataType::Binary, false),
        ];
        let schema = Schema::new(fields);
        let batch = RecordBatch::new_empty(SchemaRef::from(schema));
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        for input in input_files{
                let (key, value) = read_parquet(input);
                let key: Vec<&[u8]> = key.iter().map(|b| b.as_ref()).collect();
                let vals: Vec<&[u8]> = value.iter().map(|b| b.as_ref()).collect();
                let ids = BinaryArray::from(key);
                let vals = BinaryArray::from(vals);
                let batch = RecordBatch::try_from_iter(vec![
                        ("id", Arc::new(ids) as ArrayRef),
                        ("val", Arc::new(vals) as ArrayRef),
                ]).unwrap();
                writer.write(&batch).expect("Writing batch");
        }

        writer.close().unwrap();
}