#!/usr/bin/env ruby
require 'bundler/setup'
require 'aws-sdk-sqs'

queue_url = "https://sqs.us-east-2.amazonaws.com/115154022797/aeso-sns-sqs-github"
sqs = Aws::SQS::Client.new(region: 'us-east-2')
paths = []
receipt_handles = []

loop do
  receive_message_result = sqs.receive_message({
    queue_url: queue_url,
    max_number_of_messages: 10,
    wait_time_seconds: 0 # Do not wait to check for the message.
  })
  receive_message_result.messages.each do |message|
    receipt_handles << message.receipt_handle
    body = message.body
    next unless body =~ /Last Update : (.*?)"\r\n/
    time = Time.strptime($1, "%B %d, %Y %H:%M")
    path = time.strftime("%Y-%m-%d/%H:%M.csv")
    dir = File.dirname(path)
    Dir.mkdir(dir) unless Dir.exist?(dir)
    File.write(path, message.body)
    puts path

    paths << path
  end
  break if receive_message_result.messages.length <10
end

raise 'no files' if paths.empty?

system "git", "add", "-v", *paths
system "git", "commit", "-m", "Updated data from AWS SQS"
system "git", "push", exception: true

i=0
receipt_handles.each_slice(10) do |batch|
  sqs.delete_message_batch({
    queue_url: queue_url,
    entries: batch.map do |receipt_handle|
      {
        id: (i += 1).to_s,
        receipt_handle:
      }
    end
  })
  puts "delete"
end
