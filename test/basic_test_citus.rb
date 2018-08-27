require_relative "test_citus_helper"

class BasicCitusTest < Minitest::Test
  def test_citus
    assert PgHero.citus_enabled?
  end

  def test_database_size
    assert PgHero.database_size
  end

  def test_relation_sizes
    assert PgHero.relation_sizes
  end

  def test_citus_worker_count
  	assert_equal 1, PgHero.citus_worker_count
  end

  def test_citus_worker_settings
  	assert PgHero.citus_worker_settings
  end

  def test_citus_worker_connection_sources
    assert PgHero.citus_worker_connection_sources
  end

  def test_table_hit_rate
    assert PgHero.table_hit_rate
  end

  def test_table_caching
    assert PgHero.table_caching
  end

  def test_unused_tables
    assert PgHero.unused_tables
  end

  def test_table_stats
    assert PgHero.table_stats
  end
end
