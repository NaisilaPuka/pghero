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

      def worker_connection_sources(nodesno)
        src = Array.new(nodesno)
        y = 0
        while y < nodesno
          src[y] = select_all <<-SQL 
            with connections_stats as (select result from run_command_on_workers($cmd$ select json_agg(row_to_json(d)) from (SELECT datname AS database, usename AS user, application_name AS source, client_addr AS ip, COUNT(*) AS total_connections FROM pg_stat_activity GROUP BY 1, 2, 3, 4 ORDER BY 5 DESC, 1, 2, 3, 4) d $cmd$) limit 1 offset y) select x.database, x.user, x.source, x.ip, x.total_connections from connections_stats cross join json_to_recordset(connections_stats.result::json) as x("database" name, "user" name, "source" text, "ip" inet, "total_connections" int) 
          SQL
          y = y + 1
        end
        return src
      end
    end
  end
end
