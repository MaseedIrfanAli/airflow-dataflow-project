import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import date

class TransformData(beam.DoFn):
    def process(self, element):
        element['total_amount'] = element['quantity'] * element['price']
        element['processed_date'] = str(date.today())
        yeield element

def run():
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (
          p
          | "ReadBQ" >> beam.io.ReadFromBigQuery(
              query = """
              SELECT order_id, product, quantity, price
               from dataset.sales_raw""",
               use_standard_sql=True
        )
        | "Transform" >> beam.ParDo(TransformFata())
        | "WriteBQ" >> beam.io.WriteToBigQuery(
            "dataset.sales_analytics",
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED"
            )
        )
if __name__ == "__main__":
    run()
