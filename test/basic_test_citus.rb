require_relative "test_citus_helper"

class BasicCitusTest < Minitest::Test
  def test_citus
    assert_equal true, PgHero.citus_enabled?
  end

  def test_database_size
    assert PgHero.database_size
  end

  def test_relation_sizes
    assert PgHero.relation_sizes
  end
end