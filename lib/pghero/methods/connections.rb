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
            result, nodeid
          FROM
            (SELECT (run_command_on_workers($cmd$ SELECT
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
                                             5 DESC, 1, 2, 3, 4) worker_stat_activity $cmd$)).* ) w
          JOIN
            pg_dist_node pn ON (pn.nodename = w.nodename AND pn.nodeport = w.nodeport)
        SQL

        worker_conn_sources.map! {|wcs| select_all <<-SQL
          SELECT
            nodeid,
            (json_array_elements(result::json)->>'database')::name AS database,
            (json_array_elements(result::json)->>'user')::name AS user,
            (json_array_elements(result::json)->>'source')::text AS source,
            (json_array_elements(result::json)->>'ip')::inet AS ip,
            (json_array_elements(result::json)->>'total_connections')::int AS total_connections
          FROM
            (SELECT #{wcs[:nodeid]} AS nodeid, '#{wcs[:result]}' AS result) w
          WHERE
            w.result != ''
          SQL
        }
      end
    end
  end
end
