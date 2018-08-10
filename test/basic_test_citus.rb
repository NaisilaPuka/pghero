require_relative "test_citus_helper"

class BasicCitusTest < Minitest::Test
  def test_citus
    assert_equal true, PgHero.citus_enabled?
  end
end