require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"
require "pg_query"
require "activerecord-import"

# for Minitest < 5
Minitest::Test = MiniTest::Unit::TestCase unless defined?(Minitest::Test)

ActiveRecord::Base.establish_connection adapter: "postgresql", database: "pghero_test"

ActiveRecord::Migration.enable_extension "citus"

# Add a worker
ActiveRecord::Base.connection.execute("SELECT * from master_add_node('localhost',5433)")

ActiveRecord::Migration.create_table :users, force: true do |t|
  t.integer :city_id
  t.integer :login_attempts
  t.string :email
  t.string :zip_code
  t.boolean :active
  t.timestamp :created_at
  t.timestamp :updated_at
end

# Distribute a table
ActiveRecord::Base.connection.execute("SELECT create_distributed_table('users','id')")

# Create a distributed index
ActiveRecord::Migration.add_index :users, :updated_at

ActiveRecord::Migration.enable_extension "pg_stat_statements"

ActiveRecord::Migration.create_table :cities, force: true do |t|
  t.string :name
end

ActiveRecord::Base.connection.execute("SELECT create_distributed_table('cities','id')")

ActiveRecord::Migration.create_table :states, force: true do |t|
  t.string :name
end

ActiveRecord::Base.connection.execute("SELECT create_distributed_table('states','id')")

class User < ActiveRecord::Base
end

class City < ActiveRecord::Base
end

class State < ActiveRecord::Base
end

users =
  5000.times.map do |i|
    city_id = i % 100
    {
      city_id: city_id,
      email: "person#{i}@example.org",
      login_attempts: rand(30),
      zip_code: i % 40 == 0 ? nil : "12345",
      active: true,
      created_at: Time.now - rand(50).days,
      updated_at: Time.now - rand(50).days
    }
  end
User.import users, validate: false
ActiveRecord::Base.connection.execute("ANALYZE users")

states =
  50.times.map do |i|
    {
      name: "State #{i}"
    }
  end
State.import states, validate: false
ActiveRecord::Base.connection.execute("ANALYZE states")
