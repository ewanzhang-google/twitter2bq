from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import apache_beam as beam
import datetime
import unittest

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

exec_timestamp = datetime.datetime.now()

raw_tweets = [
      {'twitter_response': [{
            'data': 
            [{'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 0}, 'text': "Met with @Google Korea’s Harrison Kim to discuss Korea's global business opportunities and the role that technology and smart regulation can play in fostering innovation, jobs and growth. https://t.co/2IkqQkfxgU", 'id': '1613249565074808833', 'author_id': '2546833752', 'created_at': '2023-01-11T19:00:43.000Z', 'edit_history_tweet_ids': ['1613249565074808833']}, 
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 0}, 'text': '@Google’s parent company launches ‘Mineral’ with a mission to boost global farm sustainability. Defined by @Theteamatx\xa0as "a radical solution," Mineral\'s innovative analysis tools are built around advanced machine learning and AI.\n\nRead more ➡️ https://t.co/egNaqugkyh', 'id': '1613249430773436417', 'author_id': '130264658', 'created_at': '2023-01-11T19:00:11.000Z', 'edit_history_tweet_ids': ['1613249430773436417']}, 
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 0}, 'text': "Hey guys attention please. At first I'll say don't buy apple device from flipkart. After buying Iphone 13 I have facing so many issues. Apple service center hod. behavior not good. But apple support team try to help.\n@Apple @AppleSupport @SamsungMobile @Google @flipkartsupport", 'id': '1613249405091721216', 'author_id': '1600908780878712832', 'created_at': '2023-01-11T19:00:05.000Z', 'edit_history_tweet_ids': ['1613249405091721216']}, 
            {'public_metrics': {'retweet_count': 0, 'reply_count': 1, 'like_count': 0, 'quote_count': 0, 'impression_count': 5}, 'text': '1/3\n"#Cloud #Computing 2022 market growing despite geopolitical and economic instability"\n\nWith a simple search you will notice that the Big-Tech @amazon @Microsoft @Google/@AlphabetInc @Apple have managed to\nmaintain despite an unprecedented bear market&gt;', 'id': '1613249386024165382', 'author_id': '1453665345105977344', 'created_at': '2023-01-11T19:00:01.000Z', 'edit_history_tweet_ids': ['1613249386024165382']}, 
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 8}, 'text': 'How can #government organizations reduce costs and mitigate risks by migrating @SAP applications and business data to @Google Cloud? Explore our latest report to learn more: \nhttps://t.co/t5iM8zmSmY', 'id': '1613249385676308480', 'author_id': '191559783', 'created_at': '2023-01-11T19:00:00.000Z', 'edit_history_tweet_ids': ['1613249385676308480']}, 
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 2}, 'text': '@solcrash @Google @ipedros', 'id': '1613249142029180930', 'author_id': '1547081595365953537', 'created_at': '2023-01-11T18:59:02.000Z', 'edit_history_tweet_ids': ['1613249142029180930']}, 
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 2}, 'text': '@ChloeCondon @Google @lifeatgoogle Wish I could find a picture of the original 2004-2005 Google shuttle bus. It was way way less flashy.', 'id': '1613248926450073600', 'author_id': '13891402', 'created_at': '2023-01-11T18:58:11.000Z', 'edit_history_tweet_ids': ['1613248926450073600']}, 
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 3}, 'text': '@MickyWhiteNV @rickytwrites @Google @googlemaps When I lived in Taiwan I was shocked to see these symbols on the Buddhist temples. I hate how a crazy Austrian made the mirror image of this icon into an icon of hate. It stinks that hundreds of years of iconography can be tainted so quickly.', 'id': '1613248845848391681', 'author_id': '22588222', 'created_at': '2023-01-11T18:57:52.000Z', 'edit_history_tweet_ids': ['1613248845848391681']}, 
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 0}, 'text': '@Google Nope, just does not appear', 'id': '1613248816676851712', 'author_id': '1510034971964100613', 'created_at': '2023-01-11T18:57:45.000Z', 'edit_history_tweet_ids': ['1613248816676851712']}, 
            {'public_metrics': {'retweet_count': 0, 'reply_count': 1, 'like_count': 0, 'quote_count': 0, 'impression_count': 116}, 'text': '@WatcherGuru top signal, like that time @Google offered solana staking', 'id': '1613248619439611913', 'author_id': '943814112366288896', 'created_at': '2023-01-11T18:56:58.000Z', 'edit_history_tweet_ids': ['1613248619439611913']}
            ], 
            'includes': {
              'users': [{'id': '2546833752', 'name': 'Under Secretary Jose W. Fernandez', 'username': 'State_E'}, 
              {'id': '130264658', 'name': 'Farm Industry News', 'username': 'FarmIndustryNew'}, 
              {'id': '1600908780878712832', 'name': 'The Crazy Xplorer', 'username': 'thecrazyxplorer'}, 
              {'id': '1453665345105977344', 'name': '@EPAMINONDA671', 'username': 'EPAMINONDA671'}, 
              {'id': '191559783', 'name': 'Deloitte Technology', 'username': 'DeloitteOnTech'}, 
              {'id': '1547081595365953537', 'name': 'Kori', 'username': '0xKori_sol'}, 
              {'id': '13891402', 'name': 'Kelly C.', 'username': 'kacphl'}, 
              {'id': '22588222', 'name': 'Mr. Rogers', 'username': 'VegasMrr0ng'}, 
              {'id': '1510034971964100613', 'name': 'Nick moretti', 'username': 'DogeKingII1'}, 
              {'id': '943814112366288896', 'name': 'anatolie', 'username': 'anatoli_e'}]
              }, 
            'meta': {
              'newest_id': '1613249565074808833', 'oldest_id': '1613248619439611913', 'result_count': 10, 'next_token': 'b26v89c19zqg8o3fqk40lvwhgj776nav7ldxhu0p09e2l'}
              }], 
        'query': '@Google -is:retweet'}
    ]

parsed_tweets = [
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 0}, 'text': "Met with @Google Korea’s Harrison Kim to discuss Korea's global business opportunities and the role that technology and smart regulation can play in fostering innovation, jobs and growth. https://t.co/2IkqQkfxgU", 'id': '1613249565074808833', 'author_id': '2546833752', 'created_at': '2023-01-11T19:00:43.000Z', 'edit_history_tweet_ids': ['1613249565074808833'], 'query': '@Google -is:retweet'},
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 0}, 'text': '@Google’s parent company launches ‘Mineral’ with a mission to boost global farm sustainability. Defined by @Theteamatx\xa0as "a radical solution," Mineral\'s innovative analysis tools are built around advanced machine learning and AI.\n\nRead more ➡️ https://t.co/egNaqugkyh', 'id': '1613249430773436417', 'author_id': '130264658', 'created_at': '2023-01-11T19:00:11.000Z', 'edit_history_tweet_ids': ['1613249430773436417'], 'query': '@Google -is:retweet'},
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 0}, 'text': "Hey guys attention please. At first I'll say don't buy apple device from flipkart. After buying Iphone 13 I have facing so many issues. Apple service center hod. behavior not good. But apple support team try to help.\n@Apple @AppleSupport @SamsungMobile @Google @flipkartsupport", 'id': '1613249405091721216', 'author_id': '1600908780878712832', 'created_at': '2023-01-11T19:00:05.000Z', 'edit_history_tweet_ids': ['1613249405091721216'], 'query': '@Google -is:retweet'},
            {'public_metrics': {'retweet_count': 0, 'reply_count': 1, 'like_count': 0, 'quote_count': 0, 'impression_count': 5}, 'text': '1/3\n"#Cloud #Computing 2022 market growing despite geopolitical and economic instability"\n\nWith a simple search you will notice that the Big-Tech @amazon @Microsoft @Google/@AlphabetInc @Apple have managed to\nmaintain despite an unprecedented bear market&gt;', 'id': '1613249386024165382', 'author_id': '1453665345105977344', 'created_at': '2023-01-11T19:00:01.000Z', 'edit_history_tweet_ids': ['1613249386024165382'], 'query': '@Google -is:retweet'},
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 8}, 'text': 'How can #government organizations reduce costs and mitigate risks by migrating @SAP applications and business data to @Google Cloud? Explore our latest report to learn more: \nhttps://t.co/t5iM8zmSmY', 'id': '1613249385676308480', 'author_id': '191559783', 'created_at': '2023-01-11T19:00:00.000Z', 'edit_history_tweet_ids': ['1613249385676308480'], 'query': '@Google -is:retweet'},
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 2}, 'text': '@solcrash @Google @ipedros', 'id': '1613249142029180930', 'author_id': '1547081595365953537', 'created_at': '2023-01-11T18:59:02.000Z', 'edit_history_tweet_ids': ['1613249142029180930'], 'query': '@Google -is:retweet'},
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 2}, 'text': '@ChloeCondon @Google @lifeatgoogle Wish I could find a picture of the original 2004-2005 Google shuttle bus. It was way way less flashy.', 'id': '1613248926450073600', 'author_id': '13891402', 'created_at': '2023-01-11T18:58:11.000Z', 'edit_history_tweet_ids': ['1613248926450073600'], 'query': '@Google -is:retweet'},
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 3}, 'text': '@MickyWhiteNV @rickytwrites @Google @googlemaps When I lived in Taiwan I was shocked to see these symbols on the Buddhist temples. I hate how a crazy Austrian made the mirror image of this icon into an icon of hate. It stinks that hundreds of years of iconography can be tainted so quickly.', 'id': '1613248845848391681', 'author_id': '22588222', 'created_at': '2023-01-11T18:57:52.000Z', 'edit_history_tweet_ids': ['1613248845848391681'], 'query': '@Google -is:retweet'},
            {'public_metrics': {'retweet_count': 0, 'reply_count': 0, 'like_count': 0, 'quote_count': 0, 'impression_count': 0}, 'text': '@Google Nope, just does not appear', 'id': '1613248816676851712', 'author_id': '1510034971964100613', 'created_at': '2023-01-11T18:57:45.000Z', 'edit_history_tweet_ids': ['1613248816676851712'], 'query': '@Google -is:retweet'},
            {'public_metrics': {'retweet_count': 0, 'reply_count': 1, 'like_count': 0, 'quote_count': 0, 'impression_count': 116}, 'text': '@WatcherGuru top signal, like that time @Google offered solana staking', 'id': '1613248619439611913', 'author_id': '943814112366288896', 'created_at': '2023-01-11T18:56:58.000Z', 'edit_history_tweet_ids': ['1613248619439611913'], 'query': '@Google -is:retweet'}
        ]

parsed_users = [
            {'id': '2546833752', 'name': 'Under Secretary Jose W. Fernandez', 'username': 'State_E'},
            {'id': '130264658', 'name': 'Farm Industry News', 'username': 'FarmIndustryNew'},
            {'id': '1600908780878712832', 'name': 'The Crazy Xplorer', 'username': 'thecrazyxplorer'},
            {'id': '1453665345105977344', 'name': '@EPAMINONDA671', 'username': 'EPAMINONDA671'},
            {'id': '191559783', 'name': 'Deloitte Technology', 'username': 'DeloitteOnTech'},
            {'id': '1547081595365953537', 'name': 'Kori', 'username': '0xKori_sol'},
            {'id': '13891402', 'name': 'Kelly C.', 'username': 'kacphl'},
            {'id': '22588222', 'name': 'Mr. Rogers', 'username': 'VegasMrr0ng'},
            {'id': '1510034971964100613', 'name': 'Nick moretti', 'username': 'DogeKingII1'},
            {'id': '943814112366288896', 'name': 'anatolie', 'username': 'anatoli_e'}
        ]

timestamped_tweets = [
            {'id': '1613249565074808833', 'text': "Met with @Google Korea’s Harrison Kim to discuss Korea's global business opportunities and the role that technology and smart regulation can play in fostering innovation, jobs and growth. https://t.co/2IkqQkfxgU", 'author_id': '2546833752', 'created_at': '2023-01-11T19:00:43.000Z', 'exec_timestamp': exec_timestamp, 'query': '@Google -is:retweet', 'url': 'https://twitter.com/twitter/status/1613249565074808833'},
            {'id': '1613249430773436417', 'text': '@Google’s parent company launches ‘Mineral’ with a mission to boost global farm sustainability. Defined by @Theteamatx\xa0as "a radical solution," Mineral\'s innovative analysis tools are built around advanced machine learning and AI.\n\nRead more ➡️ https://t.co/egNaqugkyh', 'author_id': '130264658', 'created_at': '2023-01-11T19:00:11.000Z', 'exec_timestamp': exec_timestamp, 'query': '@Google -is:retweet', 'url': 'https://twitter.com/twitter/status/1613249430773436417'},
            {'id': '1613249405091721216', 'text': "Hey guys attention please. At first I'll say don't buy apple device from flipkart. After buying Iphone 13 I have facing so many issues. Apple service center hod. behavior not good. But apple support team try to help.\n@Apple @AppleSupport @SamsungMobile @Google @flipkartsupport", 'author_id': '1600908780878712832', 'created_at': '2023-01-11T19:00:05.000Z', 'exec_timestamp': exec_timestamp, 'query': '@Google -is:retweet', 'url': 'https://twitter.com/twitter/status/1613249405091721216'},
            {'id': '1613249386024165382', 'text': '1/3\n"#Cloud #Computing 2022 market growing despite geopolitical and economic instability"\n\nWith a simple search you will notice that the Big-Tech @amazon @Microsoft @Google/@AlphabetInc @Apple have managed to\nmaintain despite an unprecedented bear market&gt;', 'author_id': '1453665345105977344', 'created_at': '2023-01-11T19:00:01.000Z', 'exec_timestamp': exec_timestamp, 'query': '@Google -is:retweet', 'url': 'https://twitter.com/twitter/status/1613249386024165382'},
            {'id': '1613249385676308480', 'text': 'How can #government organizations reduce costs and mitigate risks by migrating @SAP applications and business data to @Google Cloud? Explore our latest report to learn more: \nhttps://t.co/t5iM8zmSmY', 'author_id': '191559783', 'created_at': '2023-01-11T19:00:00.000Z', 'exec_timestamp': exec_timestamp, 'query': '@Google -is:retweet', 'url': 'https://twitter.com/twitter/status/1613249385676308480'},
            {'id': '1613249142029180930', 'text': '@solcrash @Google @ipedros', 'author_id': '1547081595365953537', 'created_at': '2023-01-11T18:59:02.000Z', 'exec_timestamp': exec_timestamp, 'query': '@Google -is:retweet', 'url': 'https://twitter.com/twitter/status/1613249142029180930'},
            {'id': '1613248926450073600', 'text': '@ChloeCondon @Google @lifeatgoogle Wish I could find a picture of the original 2004-2005 Google shuttle bus. It was way way less flashy.', 'author_id': '13891402', 'created_at': '2023-01-11T18:58:11.000Z', 'exec_timestamp': exec_timestamp, 'query': '@Google -is:retweet', 'url': 'https://twitter.com/twitter/status/1613248926450073600'},
            {'id': '1613248845848391681', 'text': '@MickyWhiteNV @rickytwrites @Google @googlemaps When I lived in Taiwan I was shocked to see these symbols on the Buddhist temples. I hate how a crazy Austrian made the mirror image of this icon into an icon of hate. It stinks that hundreds of years of iconography can be tainted so quickly.', 'author_id': '22588222', 'created_at': '2023-01-11T18:57:52.000Z', 'exec_timestamp': exec_timestamp, 'query': '@Google -is:retweet', 'url': 'https://twitter.com/twitter/status/1613248845848391681'},
            {'id': '1613248816676851712', 'text': '@Google Nope, just does not appear', 'author_id': '1510034971964100613', 'created_at': '2023-01-11T18:57:45.000Z', 'exec_timestamp': exec_timestamp, 'query': '@Google -is:retweet', 'url': 'https://twitter.com/twitter/status/1613248816676851712'},
            {'id': '1613248619439611913', 'text': '@WatcherGuru top signal, like that time @Google offered solana staking', 'author_id': '943814112366288896', 'created_at': '2023-01-11T18:56:58.000Z', 'exec_timestamp': exec_timestamp, 'query': '@Google -is:retweet', 'url': 'https://twitter.com/twitter/status/1613248619439611913'}
]

timestamped_users = [
            {'id': '2546833752', 'name': 'Under Secretary Jose W. Fernandez', 'username': 'State_E', 'exec_timestamp': exec_timestamp},
            {'id': '130264658', 'name': 'Farm Industry News', 'username': 'FarmIndustryNew', 'exec_timestamp': exec_timestamp},
            {'id': '1600908780878712832', 'name': 'The Crazy Xplorer', 'username': 'thecrazyxplorer', 'exec_timestamp': exec_timestamp},
            {'id': '1453665345105977344', 'name': '@EPAMINONDA671', 'username': 'EPAMINONDA671', 'exec_timestamp': exec_timestamp},
            {'id': '191559783', 'name': 'Deloitte Technology', 'username': 'DeloitteOnTech', 'exec_timestamp': exec_timestamp},
            {'id': '1547081595365953537', 'name': 'Kori', 'username': '0xKori_sol', 'exec_timestamp': exec_timestamp},
            {'id': '13891402', 'name': 'Kelly C.', 'username': 'kacphl', 'exec_timestamp': exec_timestamp},
            {'id': '22588222', 'name': 'Mr. Rogers', 'username': 'VegasMrr0ng', 'exec_timestamp': exec_timestamp},
            {'id': '1510034971964100613', 'name': 'Nick moretti', 'username': 'DogeKingII1', 'exec_timestamp': exec_timestamp},
            {'id': '943814112366288896', 'name': 'anatolie', 'username': 'anatoli_e', 'exec_timestamp': exec_timestamp}
]

class TweetParseTest(unittest.TestCase):

  def test_tweets(self):
    
    # Create a test pipeline.
    with TestPipeline() as p:

      # Create an input PCollection.
      input = p | beam.Create(raw_tweets)

      # Apply the Count transform under test.
      output = input | beam.ParDo(ParseTweet())


      # Assert on the results.
      assert_that(
        output,
        equal_to(parsed_tweets))


class UserParseTest(unittest.TestCase):

  def test_user(self):
    
    # Create a test pipeline.
    with TestPipeline() as p:

      # Create an input PCollection.
      input = p | beam.Create(raw_tweets)

      # Apply the Count transform under test.
      output = input | beam.ParDo(ParseUser())

      # Assert on the results.
      assert_that(
        output,
        equal_to(parsed_users))

class TweetDictTest(unittest.TestCase):

  def test_tweets(self):
    
    # Create a test pipeline.
    with TestPipeline() as p:

      # Create an input PCollection.
      input = p | beam.Create(parsed_tweets)

      # Apply the Count transform under test.
      output = input | beam.ParDo(ToDictTweet(exec_timestamp))


      # Assert on the results.
      assert_that(
        output,
        equal_to(timestamped_tweets))


class UserDictTest(unittest.TestCase):

  def test_user(self):
    
    # Create a test pipeline.
    with TestPipeline() as p:

      # Create an input PCollection.
      input = p | beam.Create(parsed_users)

      # Apply the Count transform under test.
      output = input | beam.ParDo(ToDictUser(exec_timestamp))

      # Assert on the results.
      assert_that(
        output,
        equal_to(timestamped_users))
          
if __name__ == '__main__':
    unittest.main()