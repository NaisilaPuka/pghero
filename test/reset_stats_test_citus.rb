require_relative "test_citus_helper"

class ResetStatsCitusTest < Minitest::Test
  def setup
    PgHero.reset_stats
  end

  def test_reset_stats
    assert_equal 0.0, PgHero.index_hit_rate
  end
end
