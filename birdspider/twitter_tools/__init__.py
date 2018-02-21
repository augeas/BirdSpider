"""
Fields of interest from twitter users and tweets returned by Twython.
Extra fields provided by the various render methods are appended.
"""
twitterUserFields = [u'id', u'id_str', u'verified', u'profile_image_url_https', u'followers_count', u'listed_count',
u'utc_offset',u'statuses_count', u'description', u'friends_count', u'location', u'profile_image_url', u'geo_enabled',
u'screen_name', u'lang',u'favourites_count',u'name', u'url', u'created_at', u'time_zone', u'protected'] + [u'isotime',u'last_scraped',u'__temp_label__']

tweetFields = [u'text',u'in_reply_to_status_id',u'id',u'favorite_count',u'source',u'retweeted',
    u'in_reply_to_screen_name',u'id_str',u'retweet_count',u'in_reply_to_user_id',u'favorited',
    u'in_reply_to_user_id_str',u'possibly_sensitive',u'lang',u'created_at',u'in_reply_to_status_id_str'] + [u'isotime',u'last_scraped',u'__temp_label__']