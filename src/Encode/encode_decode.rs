use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use parquet::arrow::ArrowWriter;
use arrow::array::{Int32Array, ArrayRef, PrimitiveArray, Array};

use arrow::array::BinaryArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use bytes::Bytes;
use std::error::Error;
use std::io::prelude::*;
use aws_smithy_types::byte_stream::{ByteStream, Length};


use std::process;
use aws_sdk_s3::config::{Builder, Credentials};
use uuid::Uuid;

// fn main() {
//         let key = vec![Bytes::from("ball"), Bytes::from("bat"), Bytes::from("glove"), Bytes::from("glove")];
//         let value = vec![Bytes::from("ayo huh"), Bytes::from("ayo 121huh"), Bytes::from("ayuh"), Bytes::from("ayh")];
//         let key2 = vec![Bytes::from("O promised consort"), Bytes::from("let"), Bytes::from("us"), Bytes::from("go")];
//         let value2 = vec![Bytes::from("If you grieve"), Bytes::from("for this world"), Bytes::from("yield the path"), Bytes::from("forward to us")];
//         write_parquet("output.parquet", key, value);
//         write_parquet("output2.parquet", key2, value2);
//         let res = read_parquet("output.parquet");
//         let res2 = read_parquet("output2.parquet");
//         combine_parquets(vec!["output.parquet", "output2.parquet"], "output_combined.parquet");
//         let res3 = read_parquet("output_combined.parquet");
//         let res4 = batch_reading_parquet("output_combined.parquet", 2, 20);
//         // println!("Read from parquet: {:?}", res);
//         // println!("Read from parquet: {:?}", res2);
//         println!("Read from parquet: {:?}", res3);
//         println!("Read from parquet: {:?}", res4);
//
//
// }





// Will return from start to start + batch_size, if batch size exceeds the number of rows, it will return the rest of the rows
fn batch_reading_parquet(filename: &str, start: usize, batch_size: usize) -> (Vec<Bytes>, Vec<Bytes>){
        let file = File::open(filename).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();
        let mut ret_key = Vec::new();
        let mut ret_value = Vec::new();
        let record_batch = reader.next().unwrap().unwrap();
        let row_size = record_batch.num_rows();
        let actual_batch_size = if start + batch_size > row_size {
                row_size - start
        } else {
                batch_size
        };
        let key_arr_ref = record_batch.column(0).slice(start, actual_batch_size);
        let value_arr_ref = record_batch.column(1).slice(start, actual_batch_size);
        let key = arr_to_vec(key_arr_ref.as_any().downcast_ref::<BinaryArray>().unwrap());
        let value = arr_to_vec(value_arr_ref.as_any().downcast_ref::<BinaryArray>().unwrap());
        ret_key.extend(key);
        ret_value.extend(value);
        return (ret_key, ret_value);

}
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


//filename has to be the same name as upload to s3
pub async fn upload_parts(client: &Client, bucket: &str, filename: &str)-> Result<(), Box<dyn std::error::Error>> {
        //Split into 5MB chunks
        const CHUNK_SIZE: u64 = 1024 * 1024 * 5;
        const MAX_CHUNKS: u64 = 10000;

        let destination_filename = filename;
        let multipart_upload_res: CreateMultipartUploadOutput = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(destination_filename)
            .send()
            .await
            .unwrap();

        println!("Created multipart upload with ID: {}", multipart_upload_res.upload_id.as_ref().unwrap());


        let upload_id = multipart_upload_res.upload_id.unwrap();

        let path = Path::new(filename);
        let file_size = tokio::fs::metadata(path)
            .await
            .expect("it exists I swear")
            .len();

        let mut chunk_count = (file_size / CHUNK_SIZE) + 1;
        let mut size_of_last_chunk = file_size % CHUNK_SIZE;
        if size_of_last_chunk == 0 {
                size_of_last_chunk = CHUNK_SIZE;
                chunk_count -= 1;
        }

        if file_size == 0 {
                panic!("Bad file size.");
        }
        if chunk_count > MAX_CHUNKS {
                panic!("Too many chunks! Try increasing your chunk size.")
        }

        let mut upload_parts: Vec<CompletedPart> = Vec::new();

        for chunk_index in 0..chunk_count {
                let this_chunk = if chunk_count - 1 == chunk_index {
                        size_of_last_chunk
                } else {
                        CHUNK_SIZE
                };
                let stream = ByteStream::read_from()
                    .path(path)
                    .offset(chunk_index * CHUNK_SIZE)
                    .length(Length::Exact(this_chunk))
                    .build()
                    .await
                    .unwrap();
                //Chunk index needs to start at 0, but part numbers start at 1.
                let part_number = (chunk_index as i32) + 1;
                // snippet-start:[rust.example_code.s3.upload_part]
                let upload_part_res = client
                    .upload_part()
                    .key(filename)
                    .bucket(bucket)
                    .upload_id(upload_id.clone())
                    .body(stream)
                    .part_number(part_number)
                    .send()
                    .await;



                upload_parts.push(
                        CompletedPart::builder()
                            .e_tag(upload_part_res.unwrap().e_tag.unwrap_or_default())
                            .part_number(part_number)
                            .build(),
                );
                // snippet-end:[rust.example_code.s3.upload_part]
        }


        // snippet-start:[rust.example_code.s3.upload_part.CompletedMultipartUpload]
        let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
            .set_parts(Some(upload_parts))
            .build();
        // snippet-end:[rust.example_code.s3.upload_part.CompletedMultipartUpload]

        // snippet-start:[rust.example_code.s3.complete_multipart_upload]
        let _complete_multipart_upload_res = client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(filename)
            .multipart_upload(completed_multipart_upload)
            .upload_id(upload_id.clone())
            .send()
            .await
            .unwrap();
        // snippet-end:[rust.example_code.s3.complete_multipart_upload]

        Ok(())
}
