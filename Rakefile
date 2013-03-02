require 'json'
require 'twitter'
require 'sequel'

Twitter.configure do |config|
	config.consumer_key = 'BoRSYcvj7zDLReWs6tnc8A'
	config.consumer_secret = '41pq5QxrdBe16KL0ajQXhHHRjKI6s2p1tJGFbqJCO0'
	config.oauth_token = '62420328-xbHqLUUe0zApJ36EFhfJ8g2bWLRsyqPGJK0awEwk'
	config.oauth_token_secret = 'iHoDg7U2ATLyUxMvaZYBKEm56KSIdbip02znquwGg'
end

if `hostname` =~ /pacific/
	request_size = 5000
	DB = Sequel.connect('postgres://uuwvvjncongytz:pDsdo-AOoNpzr2Uel9lokZSHL1@ec2-54-243-228-241.compute-1.amazonaws.com:5432/dajg4s9su7v5g')
else
	request_size = 10
	DB = Sequel.connect('sqlite://twitter.db')
end

task :create_db do |t|
	DB.create_table :twitter_verified_users do
		primary_key :twitter_id
		String :data
	end
end

desc "add new ids from the list of verified users"
task :get_verified_users do |t|
	puts "getting friend ids"
	begin
		next_cursor = nil
		begin
			puts "on cursor: #{next_cursor}"
			cursor = Twitter.friend_ids('verified', cursor: next_cursor, count: request_size)
			cursor.collection.each do |id|
				if DB[:twitter_verified_users].where(twitter_id: id).count == 0
					DB[:twitter_verified_users].insert(twitter_id: id)
				end
			end
		end while next_cursor = cursor.next
	rescue Twitter::Error::TooManyRequests => error
		puts "#{Time.now}: sleeping until #{error.rate_limit.reset_at} (#{error.rate_limit.reset_in} seconds)"
		sleep error.rate_limit.reset_in
		retry
	end
end

desc "find user ids in db and get information for them (if it isn't already there)"
task :get_user_info do |t|
	results = DB[:twitter_verified_users].where(data: nil)
	results.each do |result|
		begin
			user = Twitter.user(result[:twitter_id])
		rescue Twitter::Error::TooManyRequests => error
			puts "#{Time.now}: sleeping until #{error.rate_limit.reset_at} (#{error.rate_limit.reset_in} seconds)"
			sleep error.rate_limit.reset_in
			retry
		end
		data = user.to_hash.to_json
		DB[:twitter_verified_users].where(twitter_id: result[:twitter_id]).update(data: data)
	end
end

desc "show how many rows, and rows with full user data, are in the DB"
task :status do |t|
	total_rows = DB[:twitter_verified_users].count
	partial_rows = DB[:twitter_verified_users].where(data: nil).count
	puts "total rows:\t#{total_rows}"
	puts "partial rows:\t#{partial_rows}"
	puts "full rows:\t#{total_rows - partial_rows}"
end
