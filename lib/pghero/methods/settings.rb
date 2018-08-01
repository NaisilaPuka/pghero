module PgHero
  module Methods
    module Settings

      def settings
        names =
          if server_version_num >= 90500
            %i(
              max_connections shared_buffers effective_cache_size work_mem
              maintenance_work_mem min_wal_size max_wal_size checkpoint_completion_target
              wal_buffers default_statistics_target
            )
          else
            %i(
              max_connections shared_buffers effective_cache_size work_mem
              maintenance_work_mem checkpoint_segments checkpoint_completion_target
              wal_buffers default_statistics_target
            )
          end
        fetch_settings(names)
      end

      def citus_worker_settings
        citus_worker_names =
          if server_version_num >= 90500
            %i(
              max_connections shared_buffers effective_cache_size work_mem
              maintenance_work_mem min_wal_size max_wal_size checkpoint_completion_target
              wal_buffers default_statistics_target
            )
          else
            %i(
              max_connections shared_buffers effective_cache_size work_mem
              maintenance_work_mem checkpoint_segments checkpoint_completion_target
              wal_buffers default_statistics_target
            )
          end
        citus_fetch_worker_settings(citus_worker_names)
      end
         
      def autovacuum_settings
        fetch_settings %i(autovacuum autovacuum_max_workers autovacuum_vacuum_cost_limit autovacuum_vacuum_scale_factor autovacuum_analyze_scale_factor)
      end

      def vacuum_settings
        fetch_settings %i(vacuum_cost_limit)
      end

      private

      def fetch_settings(names)
        Hash[names.map { |name| [name, select_one("SHOW #{name}")] }]
      end

      def citus_fetch_worker_settings(citus_worker_names)
        Hash[citus_worker_names.map { |citus_worker_name| [citus_worker_name, select_one("SELECT result FROM run_command_on_workers($cmd$ SHOW #{worker_name} $cmd$) LIMIT 1")] }]
      end
    end
  end
end
