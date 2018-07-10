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
      
      def citus_worker_count
        @citus_worker_count = select_one("SELECT COUNT(*) FROM master_get_active_worker_nodes()")
      end
    end
  end
end
