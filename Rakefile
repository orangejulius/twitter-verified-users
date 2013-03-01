require 'twitter'
require 'sequel'

Twitter.configure do |config|
	config.consumer_key = 'IrA112tGcxGtY1GBWfw1Aw'
	config.consumer_secret = 'XYn5gSfg3vwMfcN2qvTnEoJWVVHuobhwzNpovws2cU'
	config.oauth_token = '15061294-M5iJIHRgpbbxyaqe1om7IDWY8G2oREwIxdkVtHpf7'
	config.oauth_token_secret = 'l782bFMpEj1P9uQmw9FemTAYLM3UGMOmIiTjPxfgnA'
end

DB = Sequel.connect('sqlite://twitter.db')

task :create_db do |t|
	DB.create_table :twitter_verified_users do
		primary_key :twitter_id
		String :screen_name
		String :real_name
		String :photo_url
	end
end

task :get_friend_ids do |t|
	puts "getting friend ids"
	begin
		next_cursor = nil
		cursor = Twitter.friend_ids('verified', cursor: next_cursor)
		begin
			cursor.collection.each do |id|
				begin
					DB[:twitter_verified_users].insert(twitter_id: id)
				rescue Exception => e
				end
			end
		end while next_cursor = cursor.next
	rescue Twitter::Error::TooManyRequests => error
		puts "sleeping for "+error.rate_limit.reset_in.to_s
		sleep error.rate_limit.reset_in
		retry
	end
end

task :check_rate_limit do |t|
	rl = Twitter::RateLimit
	puts rl.limit#, rl.remaining, rl.reset_at
end
