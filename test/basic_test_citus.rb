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

  def test_index_hit_rate
    assert PgHero.index_hit_rate
  end

  def test_index_caching
    assert PgHero.index_caching
  end

  def test_index_usage
    assert PgHero.index_usage
  end

  def test_missing_indexes
    assert PgHero.missing_indexes
  end

  def test_unused_indexes
    assert PgHero.unused_indexes
  end

  def test_transaction_id_danger
    assert PgHero.transaction_id_danger
  end

  def test_maintenance_info
    assert PgHero.maintenance_info
  end

  def test_citus_version
    assert PgHero.citus_version
  end

  def test_nodes_info
    assert PgHero.nodes_info
  end

  def test_colocated_shard_sizes
    assert PgHero.colocated_shard_sizes
  end

  def test_distributed_tables
    assert PgHero.distributed_tables
  end

  def test_shard_data_distribution
    assert PgHero.shard_data_distribution('users', 'id')
  end

  def test_dist_tables_extended
    assert PgHero.dist_tables_extended
  end

  def test_node_sizes
    assert PgHero.node_sizes
  end
end
