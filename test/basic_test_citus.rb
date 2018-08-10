require_relative "test_citus_helper"

class BasicCitusTest < Minitest::Test
  def test_citus
    assert PgHero.citus_enabled?
  end
end
