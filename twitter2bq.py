import datetime
import logging

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from searchtweets import ResultStream, gen_request_parameters, load_credentials, collect_results
from google.cloud import bigquery as bq
from google.cloud import storage
from google.cloud.exceptions import NotFound

class ParseTweet(beam.DoFn):
  def process(self, twitter_response):
    query = twitter_response["query"]
    if(len(twitter_response["twitter_response"]) == 0):
      return None
    tweets = twitter_response["twitter_response"][0]["data"]
    tweets = [dict(tweet, query=query) for tweet in tweets]
    return tweets

class ParseUser(beam.DoFn):
  def process(self, twitter_response):
    if(len(twitter_response["twitter_response"]) == 0):
      return None
    users = twitter_response["twitter_response"][0]["includes"]["users"]
    return users

class ToDictTweet(beam.DoFn):
  def __init__(self, exec_timestamp):
    self.exec_timestamp = exec_timestamp

  def process(self, elem_dict):
    return [{
      'id': elem_dict['id'],
      'text': elem_dict['text'],
      'author_id': elem_dict['author_id'],
      'created_at': elem_dict['created_at'],
      'exec_timestamp': self.exec_timestamp,
      'query': elem_dict['query'],
      'url': "https://twitter.com/twitter/status/" + elem_dict['id']
    }]

class ToDictUser(beam.DoFn):
  def __init__(self, exec_timestamp):
    self.exec_timestamp = exec_timestamp

  def process(self, elem_dict):
    return [{
      'id':elem_dict["id"],
      'name':elem_dict["name"],
      'username':elem_dict["username"],
      'exec_timestamp': self.exec_timestamp
    }]

class SearchTweets(beam.DoFn):
    def __init__(self):
      self.search_args = load_credentials("credentials.yaml",
          yaml_key="search_tweets_v2",
          env_overwrite=False)

    def process(self, query_text, lastid, max_tweets):
      logging.info("Max Tweet ID:%s"%lastid)
      results_per_call = max(min(max_tweets,100),10)
      query = gen_request_parameters(query_text, results_per_call=results_per_call, granularity=None, expansions="author_id", tweet_fields="id,created_at,public_metrics,text", since_id=lastid)
      tweets = collect_results(query,
          max_tweets=max_tweets,
          result_stream_args=self.search_args)
      init_value = [{"twitter_response": tweets, "query": query_text}]
      print (init_value)
      return [{"twitter_response": tweets, "query": query_text}]

class ApplicationOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
      '--gcp_project_id',
      required=True,
      help='Google Cloud project id.',
      type=str)
    parser.add_argument(
      '--gcs_bucket_id',
      required=True,
      help='Google Cloud Storage bucket id. Used to store configuration files and temporary data for the pipeline.',
      type=str)
    parser.add_argument(
      '--bq_project_id',
      required=True,
      help='BQ Project ID where output of pipeline will be stored.',
      type=str)
    parser.add_argument(
      '--bq_dataset_name',
      required=True,
      help='BQ Dataset name where output of pipeline will be stored.',
      type=str)
    parser.add_argument(
      '--max_tweets',
      default=100,
      help='Maximum number of tweets to pull for each query. Minimum 10. Above 100, will be rounded up to the nearest 100.',
      type=int)

def run(argv=None, save_main_session=True):
  pipeline_options = PipelineOptions(
    job_name="twitter2bq"
  )
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  application_options = pipeline_options.view_as(ApplicationOptions)

  GCP_PROJECT_ID = application_options.gcp_project_id
  GCS_BUCKET_NAME = application_options.gcs_bucket_id
  BQ_PROJECT_ID = application_options.bq_project_id
  BQ_DATASET_NAME = application_options.bq_dataset_name
  MAX_TWEETS = application_options.max_tweets

  exec_timestamp = datetime.datetime.now()

  storage_client = storage.Client(GCP_PROJECT_ID)
  bucket = storage_client.get_bucket(GCS_BUCKET_NAME)
  blob = bucket.blob("twitter2bq/credentials.yaml")
  blob.download_to_filename("credentials.yaml")

  with beam.Pipeline(options=pipeline_options) as p:
    max_id = 0
    client = bq.Client()
    table_id = "%s.%s.tweet"%(GCP_PROJECT_ID,BQ_DATASET_NAME)

    try:
        client.get_table(table_id)
        lastid = (
            p
            | 'Read From BigQuery' >> beam.io.ReadFromBigQuery(
            project=GCP_PROJECT_ID,
            query="SELECT max(id) as lastid FROM %s.tweet"%(BQ_DATASET_NAME),
            gcs_location="gs://%s/twitter2bq/tmp"%(GCS_BUCKET_NAME))
            | 'Read Last ID' >> beam.Map(lambda x: (x["lastid"]))
        )
        max_id = beam.pvalue.AsSingleton(lastid)
    except NotFound:
        logging.info("Tweets table not found. Setting max tweet id to 0.")
        max_id = 0

    raw_data = ( 
       p
        | 'Read Search Query' >> beam.io.ReadFromText("gs://%s/twitter2bq/queries.txt"%(GCS_BUCKET_NAME))
        | "SearchTweets" >> beam.ParDo(SearchTweets(), max_id, MAX_TWEETS)
    )

    tweets = (
      raw_data
      | beam.ParDo(ParseTweet())
      | beam.ParDo(ToDictTweet(exec_timestamp))
    )

    users = (
      raw_data
      | beam.ParDo(ParseUser())
      | beam.ParDo(ToDictUser(exec_timestamp))
    )

    table_spec_tweets = bigquery.TableReference(
      projectId=BQ_PROJECT_ID,
      datasetId=BQ_DATASET_NAME,
      tableId='tweets'
    )

    table_schema_tweets = 'id:INTEGER, text:STRING, author_id:INTEGER, created_at:STRING, exec_timestamp:TIMESTAMP, query:STRING, url:STRING'

    ( 
      tweets
      | 'Write to BigQuery Tweets' >> beam.io.WriteToBigQuery(
        table_spec_tweets,
        schema=table_schema_tweets,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        custom_gcs_temp_location="gs://%s/twitter2bq/tmp"%(GCS_BUCKET_NAME)
        )
    )

    table_spec_user = bigquery.TableReference(
      projectId=BQ_PROJECT_ID,
      datasetId=BQ_DATASET_NAME,
      tableId='users'
    )

    table_schema_user = 'id:STRING, name:STRING, username:STRING, exec_timestamp:TIMESTAMP'

    ( users
      | 'Write to BigQuery User' >> beam.io.WriteToBigQuery(
        table_spec_user,
        schema=table_schema_user,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        custom_gcs_temp_location="gs://%s/twitter2bq/tmp"%(GCS_BUCKET_NAME)
      )
    )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()