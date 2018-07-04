module PgHero
  module Methods
    module Citus
      # It will only cache citus_enabled if it is true once.
      def citus_enabled?
        @citus_enabled ||= citus_readable?
      end

      def citus_readable?
        select_one("SELECT EXISTS(SELECT * FROM pg_extension WHERE extname = 'citus')")
      end
    end
  end
end
