#!/usr/bin/env python3

import argparse
import logging
import io
import typing

import csv

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions


class TaxiSchema(typing.NamedTuple):
    vendor_id: typing.Optional[str]
    pickup_datetime: typing.Optional[str]
    dropoff_datetime: typing.Optional[str]
    passenger_count: typing.Optional[int]
    trip_distance: typing.Optional[int]
    rate_code: typing.Optional[str]
    store_and_fwd_flag: typing.Optional[int]
    payment_type: typing.Optional[str]
    fare_amount: typing.Optional[float]
    extra: typing.Optional[float]
    mta_tax: typing.Optional[float]
    tip_amount: typing.Optional[float]
    tolls_amount: typing.Optional[float]
    imp_surcharge: typing.Optional[float]
    total_amount: typing.Optional[float] 
    pickup_location_id: typing.Optional[int]
    dropoff_location_id: typing.Optional[int]
    data_file_year: typing.Optional[int]
    data_file_month: typing.Optional[int]


def parse_element(element):
    reader = csv.reader([element])
    fields = next(reader)
    if len(fields) != len(TaxiSchema._fields):
        raise Exception(f'Not enough fields in element: {element}')

    def check_type(field, field_type):
        typed_field = None
        try:
            typed_field = field_type(field)
        except ValueError:
            # Keep as null
            pass
        return typed_field

    result = None
    try:
        result = TaxiSchema(
            vendor_id=check_type(fields[0], str),
            pickup_datetime=check_type(fields[1].rstrip(' UTC'), str),
            dropoff_datetime=check_type(fields[2].rstrip(' UTC'), str),
            passenger_count=check_type(fields[3], int),
            trip_distance=check_type(fields[4], int),
            rate_code=check_type(fields[5], str),
            store_and_fwd_flag=check_type(fields[6], int),
            payment_type=check_type(fields[7], str),
            fare_amount=check_type(fields[8], float),
            extra=check_type(fields[9], float),
            mta_tax=check_type(fields[10], float),
            tip_amount=check_type(fields[11], float),
            tolls_amount=check_type(fields[12], float),
            imp_surcharge=check_type(fields[13], float),
            total_amount=check_type(fields[14], float), 
            pickup_location_id=check_type(fields[15], int),
            dropoff_location_id=check_type(fields[16], int),
            data_file_year=check_type(fields[17], int),
            data_file_month=check_type(fields[18], int)
        )
    except Exception as e:
        print(f'Exception: {e} \nElement: {element}')
        raise
    return result


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input GCS file pattern.'
    )
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output BigQuery table.'
    )
    parser.add_argument(
        '--classpath',
        dest='classpath',
        required=True,
        help='BigQueryIO Java transform.'
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    p = beam.Pipeline(options=pipeline_options)

    beam.coders.registry.register_coder(TaxiSchema, beam.coders.RowCoder)

    processed_records = (
        p
        | 'Read from GCS' >> beam.io.textio.ReadFromText(known_args.input, skip_header_lines=1)
        | 'Parse Element' >> beam.Map(parse_element).with_output_types(TaxiSchema)

        # Use the BigQuery Write API (via Java x-language)
        | 'Write to BigQuery' >> beam.transforms.external.JavaExternalTransform(
            'com.google.beam.bigquery.write.WriteToBigQuery',
            classpath=[known_args.classpath]
        ).to(known_args.output).usingWriteAPIAtLeastOnceMethod()

        # Use BigQuery file loads
        # | 'Convert to Dict' >> beam.Map(lambda x: dict(x._asdict()))
        # | 'Write to BigQuery' >> beam.io.gcp.bigquery.WriteToBigQuery(
        #     known_args.output,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        #     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        # )
    )

    result = p.run()
    return result


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
