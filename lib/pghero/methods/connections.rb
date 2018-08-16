module PgHero
  module Methods
    module Connections
      def total_connections
        select_one("SELECT COUNT(*) FROM pg_stat_activity")
      end

      def connection_sources
        select_all <<-SQL
          SELECT
            datname AS database,
            usename AS user,
            application_name AS source,
            client_addr AS ip,
            COUNT(*) AS total_connections
          FROM
            pg_stat_activity
          GROUP BY
            1, 2, 3, 4
          ORDER BY
            5 DESC, 1, 2, 3, 4
        SQL
      end

      def citus_worker_connection_sources
        worker_conn_sources = select_all <<-SQL
          SELECT
            result
          FROM
            run_command_on_workers($cmd$ SELECT
                                           json_agg(row_to_json(worker_stat_activity))
                                         FROM(
                                           SELECT
                                             datname AS database,
                                             usename AS user,
                                             application_name AS source,
                                             client_addr AS ip,
                                             COUNT(*) AS total_connections
                                           FROM
                                             pg_stat_activity
                                           GROUP BY
                                             1, 2, 3, 4
                                           ORDER BY
                                             5 DESC, 1, 2, 3, 4) worker_stat_activity $cmd$)
        SQL

        worker_conn_sources.map! {|wcs| select_all <<-SQL
          SELECT
            database,
            user,
            source,
            ip,
            total_connections
          FROM
            json_to_recordset('#{wcs[:result]}'::json)
            AS worker_stats("database" name, "user" name, "source" text, "ip" inet, "total_connections" int)
          SQL
        }
      end
    end
  end
end
