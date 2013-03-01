require 'json'
require 'twitter'
require 'sequel'

Twitter.configure do |config|
	config.consumer_key = 'IrA112tGcxGtY1GBWfw1Aw'
	config.consumer_secret = 'XYn5gSfg3vwMfcN2qvTnEoJWVVHuobhwzNpovws2cU'
	config.oauth_token = '15061294-M5iJIHRgpbbxyaqe1om7IDWY8G2oREwIxdkVtHpf7'
	config.oauth_token_secret = 'l782bFMpEj1P9uQmw9FemTAYLM3UGMOmIiTjPxfgnA'
end

DB =Sequel.connect('postgres://uuwvvjncongytz:pDsdo-AOoNpzr2Uel9lokZSHL1@ec2-54-243-228-241.compute-1.amazonaws.com:5432/dajg4s9su7v5g')

task :create_db do |t|
	DB.create_table :twitter_verified_users do
		primary_key :twitter_id
		String :data
	end
end

task :get_friend_ids do |t|
	puts "getting friend ids"
	begin
		next_cursor = nil
		begin
			cursor = Twitter.friend_ids('verified', cursor: next_cursor)
			cursor.collection.each do |id|
				if DB[:twitter_verified_users].where(twitter_id: id).count == 0
					DB[:twitter_verified_users].insert(twitter_id: id)
				end
			end
		end while next_cursor = cursor.next
	rescue Twitter::Error::TooManyRequests => error
		puts "sleeping for "+error.rate_limit.reset_in.to_s
		sleep error.rate_limit.reset_in
		retry
	end
end

task :get_user_info do |t|
	results = DB[:twitter_verified_users].where(data: nil)
	results.each do |result|
		begin
			user = Twitter.user(result[:twitter_id])
		rescue Twitter::Error::TooManyRequests => error
			puts "sleeping for "+error.rate_limit.reset_in.to_s
			sleep error.rate_limit.reset_in
			retry
		end
		data = user.to_hash.to_json
		DB[:twitter_verified_users].where(twitter_id: result[:twitter_id]).update(data: data)
	end
end

task :show_status do |t|
	total_rows = DB[:twitter_verified_users].count
	partial_rows = DB[:twitter_verified_users].where(data: nil).count
	puts "total rows:\t#{total_rows}"
	puts "partial rows:\t#{partial_rows}"
	puts "full rows:\t#{total_rows - partial_rows}"
end
